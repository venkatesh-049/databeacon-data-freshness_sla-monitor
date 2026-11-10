import asyncio
import csv
import io
import sqlite3
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, Response

from databeacon.db.connection import connect
from databeacon.db.repository import (
    acknowledge_alert,
    dashboard_data,
    list_alerts,
    list_rejects,
    report_data,
    seed_datasets,
)
from databeacon.pipeline.ingestion import run_pipeline
from databeacon.pipeline.generator import generate_csv
from databeacon.pipeline.monitor import monitor_expected_windows
from databeacon.observability import logger
from databeacon.settings import Settings


def create_app(settings: Settings | None = None) -> FastAPI:
    config = settings or Settings()
    config.ensure_directories()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        task = asyncio.create_task(_monitor_loop(config))
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    app = FastAPI(title="DataBeacon", lifespan=lifespan)

    @app.get("/api/health")
    def health() -> dict:
        connection = connect(config.db_path)
        connection.close()
        return {"status": "ok"}

    @app.get("/api/dashboard")
    def dashboard() -> dict:
        monitor_expected_windows(config.db_path, arrival_grace_minutes=config.arrival_grace_minutes)
        connection = connect(config.db_path)
        seed_datasets(connection)
        try:
            return dashboard_data(connection)
        finally:
            connection.close()

    @app.post("/api/monitor/run")
    def run_monitor() -> dict:
        return monitor_expected_windows(
            config.db_path, arrival_grace_minutes=config.arrival_grace_minutes
        )

    @app.get("/api/report")
    def report() -> dict:
        connection = connect(config.db_path)
        seed_datasets(connection)
        data = report_data(connection)
        connection.close()
        return data

    @app.get("/api/rejects")
    def rejects() -> list[dict]:
        connection = connect(config.db_path)
        data = list_rejects(connection)
        connection.close()
        return data

    @app.get("/api/alerts")
    def alerts() -> list[dict]:
        connection = connect(config.db_path)
        data = list_alerts(connection)
        connection.close()
        return data

    @app.get("/api/runs/{run_id}/datasets/{dataset_id}")
    def run_dataset_details(run_id: str, dataset_id: str, search: str | None = None) -> dict:
        connection = connect(config.db_path)
        try:
            run = connection.execute(
                "SELECT * FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if not run:
                raise HTTPException(status_code=404, detail="run not found")
            params: list[str] = [run_id, dataset_id]
            search_clause = ""
            if search:
                search_clause = " AND (raw_payload LIKE ? OR CAST(source_row AS TEXT) LIKE ?)"
                params.extend([f"%{search}%", f"%{search}%"])
            accepted = [dict(row) for row in connection.execute(
                """SELECT observation_id, event_key, run_id, dataset_id, source_file,
                          expected_at, arrived_at, completed_at,
                          row_count, freshness_minutes, completion_minutes, sla_status,
                          raw_payload, observed_at
                   FROM event_observations
                   WHERE run_id=? AND dataset_id=?""" + search_clause +
                " ORDER BY expected_at", params
            )]
            reject_params: list[str] = [run_id, dataset_id]
            reject_search = ""
            if search:
                reject_search = " AND (raw_payload LIKE ? OR reason_code LIKE ? OR reason_detail LIKE ?)"
                reject_params.extend([f"%{search}%"] * 3)
            rejects = [dict(row) for row in connection.execute(
                """SELECT reject_id, source_file, source_row, reason_code, reason_detail,
                          raw_payload
                   FROM rejects
                   WHERE run_id=? AND json_extract(raw_payload, '$.dataset_id')=?""" + reject_search +
                " ORDER BY source_row",
                reject_params[:2] + ([reject_params[2], reject_params[2], reject_params[2]]
                                     if search else []),
            )]
            duplicates = [dict(row) for row in connection.execute(
                """SELECT duplicate_id, run_id, dataset_id, event_key, source_file,
                          source_row, reason
                   FROM duplicate_records WHERE run_id=? AND dataset_id=?
                   ORDER BY source_row""", (run_id, dataset_id)
            )]
            dataset = connection.execute(
                "SELECT dataset_name FROM datasets WHERE dataset_id=?", (dataset_id,)
            ).fetchone()
            if not dataset:
                raise HTTPException(status_code=404, detail="dataset not found")
            return {
                "run": dict(run), "dataset_id": dataset_id,
                "dataset_name": dataset["dataset_name"],
                "accepted": accepted, "rejects": rejects, "duplicates": duplicates,
                "counts": {"accepted": len(accepted), "rejected": len(rejects),
                           "duplicates": len(duplicates)},
            }
        finally:
            connection.close()

    @app.get("/api/download/run-dataset/{run_id}/{dataset_id}/{kind}")
    def download_run_dataset(run_id: str, dataset_id: str, kind: str) -> Response:
        queries = {
            "accepted": (
                """SELECT observation_id, event_key, run_id, dataset_id, source_file,
                          expected_at, arrived_at, completed_at, row_count,
                          freshness_minutes, completion_minutes, sla_status,
                          raw_payload, observed_at
                   FROM event_observations WHERE run_id=? AND dataset_id=?
                   ORDER BY expected_at""",
                "accepted",
            ),
            "rejected": (
                """SELECT reject_id, run_id, source_file, source_row, reason_code,
                          reason_detail, raw_payload
                   FROM rejects
                   WHERE run_id=?
                     AND json_extract(raw_payload, '$.dataset_id')=?
                   ORDER BY source_row""",
                "rejected",
            ),
            "duplicates": (
                """SELECT duplicate_id, run_id, dataset_id, event_key, source_file,
                          source_row, reason
                   FROM duplicate_records WHERE run_id=? AND dataset_id=?
                   ORDER BY source_row""",
                "duplicates",
            ),
        }
        if kind not in queries:
            raise HTTPException(status_code=404, detail="unknown dataset detail type")
        query, filename_prefix = queries[kind]
        connection = connect(config.db_path)
        try:
            cursor = connection.execute(query, (run_id, dataset_id))
            output = io.StringIO(newline="")
            writer = csv.writer(output)
            writer.writerow([column[0] for column in cursor.description])
            writer.writerows([tuple(row) for row in cursor.fetchall()])
        finally:
            connection.close()
        return Response(
            content=output.getvalue(),
            media_type="text/csv",
            headers={
                "Content-Disposition":
                    f'attachment; filename="{dataset_id}_{filename_prefix}.csv"'
            },
        )

    @app.get("/api/download/{dataset}")
    def download(
        dataset: str,
        status: str | None = None,
        dataset_id: str | None = None,
        search: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> Response:
        queries = {
            "events": (
                "SELECT event_key, run_id, dataset_id, source_file, expected_at, arrived_at, "
                "completed_at, row_count, freshness_minutes, completion_minutes, sla_status "
                "FROM arrival_events",
                "events.csv",
            ),
            "observations": (
                "SELECT observation_id, event_key, run_id, dataset_id, source_file, expected_at, "
                "arrived_at, completed_at, row_count, freshness_minutes, completion_minutes, "
                "sla_status, observed_at FROM event_observations ORDER BY observation_id",
                "event_observations.csv",
            ),
            "rejects": (
                "SELECT reject_id, run_id, source_file, source_row, reason_code, reason_detail, "
                "raw_payload FROM rejects ORDER BY reject_id",
                "rejects.csv",
            ),
            "alerts": (
                "SELECT alert_id, run_id, event_key, window_key, dataset_id, severity, alert_type, "
                "message, action, created_at, acknowledged, resolved_at FROM alerts ORDER BY alert_id",
                "alerts.csv",
            ),
            "windows": (
                "SELECT window_key, dataset_id, expected_at, arrival_deadline_at, "
                "completion_deadline_at, status, last_checked_at FROM expected_windows",
                "expected_windows.csv",
            ),
        }
        if dataset not in queries:
            raise HTTPException(status_code=404, detail="unknown download dataset")
        query, filename = queries[dataset]
        filters: list[str] = []
        parameters: list[str] = []
        if dataset in {"events", "windows"}:
            if status:
                filters.append("sla_status=?" if dataset == "events" else "status=?")
                parameters.append(status)
            if dataset_id:
                filters.append("dataset_id=?")
                parameters.append(dataset_id)
            if search:
                filters.append(
                    "(dataset_id LIKE ? OR source_file LIKE ?)"
                    if dataset == "events" else "dataset_id LIKE ?"
                )
                search_value = f"%{search}%"
                parameters.extend([search_value] * (2 if dataset == "events" else 1))
            if from_date:
                filters.append("substr(expected_at, 1, 10)>=?")
                parameters.append(from_date)
            if to_date:
                filters.append("substr(expected_at, 1, 10)<=?")
                parameters.append(to_date)
            if filters:
                query += " WHERE " + " AND ".join(filters)
            query += " ORDER BY expected_at, dataset_id"
        connection = connect(config.db_path)
        try:
            cursor = connection.execute(query, parameters)
            rows = cursor.fetchall()
            output = io.StringIO(newline="")
            writer = csv.writer(output)
            writer.writerow([column[0] for column in cursor.description])
            writer.writerows([tuple(row) for row in rows])
        finally:
            connection.close()
        return Response(
            content=output.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @app.post("/api/alerts/{alert_id}/acknowledge")
    def acknowledge(alert_id: int) -> dict:
        connection = connect(config.db_path)
        updated = acknowledge_alert(connection, alert_id)
        connection.close()
        if not updated:
            raise HTTPException(status_code=404, detail="open alert not found")
        return {"alert_id": alert_id, "acknowledged": True}

    @app.post("/api/runs")
    def trigger(source_file: str) -> dict:
        path = Path(source_file)
        if not path.exists():
            raise HTTPException(status_code=400, detail=f"source file not found: {source_file}")
        return run_pipeline(config.db_path, path)

    @app.post("/api/runs/generate")
    def generate_and_run(rows: int = 12, seed: int | None = 42,
                         randomize_status: bool = False,
                         include_missing: bool = True) -> dict:
        if rows < 0 or rows > 1_000_000:
            raise HTTPException(status_code=422, detail="rows must be between 0 and 1,000,000")
        path = config.incoming_dir / f"generated_{uuid.uuid4().hex}.csv"
        generate_csv(
            path,
            rows=rows,
            seed=seed,
            append=False,
            randomize_status=randomize_status,
            include_missing=include_missing,
        )
        result = run_pipeline(config.db_path, path)
        result["source_file"] = str(path)
        result["download_url"] = "/api/download/events"
        return result

    @app.post("/api/runs/upload")
    async def upload_run(request: Request) -> dict:
        content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if content_type not in {"text/csv", "application/csv", "application/octet-stream"}:
            raise HTTPException(status_code=415, detail="upload must use a CSV content type")
        max_bytes = 25 * 1024 * 1024
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                if int(content_length) > max_bytes:
                    raise HTTPException(status_code=413, detail="CSV upload exceeds 25 MiB")
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="invalid content-length header") from exc
        content = bytearray()
        async for chunk in request.stream():
            content.extend(chunk)
            if len(content) > max_bytes:
                raise HTTPException(status_code=413, detail="CSV upload exceeds 25 MiB")
        if not content:
            raise HTTPException(status_code=400, detail="CSV upload is empty")
        path = config.incoming_dir / f"producer_{uuid.uuid4().hex}.csv"
        path.write_bytes(content)
        try:
            return await asyncio.to_thread(run_pipeline, config.db_path, path)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/", response_class=HTMLResponse)
    def home() -> str:
        return """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
        <title>DataBeacon | SLA Control Room</title>
        <style>
        :root{font-family:Inter,ui-sans-serif,system-ui,-apple-system,sans-serif;color:#e8eefc;background:#0b1220}
        *{box-sizing:border-box}html,body{width:100%;min-height:100%;overflow-x:hidden}body{margin:0;background:radial-gradient(circle at 85% -10%,#213765 0,#0b1220 38%),#0b1220}.shell{width:100%;max-width:1600px;margin:0 auto;padding:clamp(20px,2.4vw,36px) clamp(20px,3vw,48px)}
        header{display:flex;justify-content:space-between;align-items:center;margin-bottom:26px}.brand{display:flex;gap:13px;align-items:center}
        .logo{background:linear-gradient(135deg,#63e6be,#4e7cff);color:#07111e;border-radius:14px;padding:11px 13px;font-weight:900;box-shadow:0 8px 28px #4e7cff44}
        h1{font-size:27px;letter-spacing:-.7px;margin:0}h2{font-size:15px;margin:0 0 7px}.muted{color:#8999b7;font-size:12px}
        .refresh,.toolbar button{border:1px solid #344663;background:#15223a;color:#cfe0ff;border-radius:10px;padding:10px 14px;cursor:pointer}.refresh:hover,.toolbar button:hover{background:#203354}.toolbar{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.toolbar input,.toolbar select{border:1px solid #344663;background:#101a2d;color:#cfe0ff;border-radius:8px;padding:9px}.toolbar input[type=number]{width:74px}.toolbar input[type=search]{width:150px}.toolbar input[type=date]{width:128px}.toolbar select{min-width:120px}.filters{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin:12px 0}.filters label{color:#8999b7;font-size:12px}.filters input,.filters select{border:1px solid #344663;background:#101a2d;color:#cfe0ff;border-radius:8px;padding:8px}.filters button{border:1px solid #344663;background:#15223a;color:#cfe0ff;border-radius:8px;padding:8px 11px;cursor:pointer}
        .cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:18px}.card,.panel{background:linear-gradient(145deg,#131f35,#101a2d);border:1px solid #263957;border-radius:16px;padding:18px;box-shadow:0 10px 30px #00000022}
        .card{position:relative;overflow:hidden}.card:after{content:"";position:absolute;width:70px;height:70px;border-radius:50%;right:-25px;top:-28px;background:#4e7cff22}.metric{font-size:32px;font-weight:800;margin:9px 0 2px}.metric-label{color:#8ea0be;font-size:12px;text-transform:uppercase;letter-spacing:.7px}
        .layout{display:grid;grid-template-columns:minmax(0,1.55fr) minmax(0,1fr);gap:18px;margin-bottom:18px}.history-layout{display:block}.history-layout>.panel{width:100%;margin-bottom:18px}.panel-head{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:12px}
        table{border-collapse:collapse;width:100%;font-size:13px}th{text-align:left;color:#8294b5;font-weight:600;border-bottom:1px solid #263957;border-right:1px solid #263957;padding:10px 8px}th:last-child,td:last-child{border-right:0}
        td{border-bottom:1px solid #1e2d46;border-right:1px solid #263957;padding:7px 8px;line-height:1.2}tr:last-child td{border-bottom:0}.pill{display:inline-block;border-radius:20px;padding:5px 9px;font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:.5px}
        .fresh{background:#143d3b;color:#63e6be}.late{background:#48351b;color:#ffd27a}.missing{background:#4b202e;color:#ff91a9}.incomplete{background:#48351b;color:#ffd27a}.scheduled{background:#21304a;color:#a8b8d5}
        .critical{color:#ff91a9}.warning{color:#ffd27a}.alert{display:flex;justify-content:space-between;gap:12px;padding:13px 0;border-bottom:1px solid #1e2d46}
        .alert:last-child{border:0}.alert b{font-size:10px;text-transform:uppercase;letter-spacing:.5px}.action{font-size:12px;color:#8ea0be;margin-top:4px;line-height:1.4}.ack{border:0;background:#223a6c;color:#bcd2ff;border-radius:7px;padding:6px 9px;cursor:pointer;white-space:nowrap}
        .healthbar{height:7px;background:#23324b;border-radius:9px;overflow:hidden;margin-top:12px}.healthfill{height:100%;background:linear-gradient(90deg,#63e6be,#4e7cff);border-radius:9px}.legend{display:flex;gap:12px;margin-top:12px;font-size:11px;color:#93a3bf}.dot{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:4px}
        pre{font-size:11px;white-space:pre-wrap;color:#a7b6d0;max-height:180px;overflow:auto}.table-scroll{height:320px;overflow:auto;border:1px solid #263957;border-radius:10px}.table-scroll table{min-width:650px}.table-scroll th{position:sticky;top:0;background:#131f35;z-index:1}.alerts-scroll{height:320px;overflow-y:auto;padding-right:8px}.run{border:1px solid #263957;border-radius:10px;margin-top:10px;overflow:hidden}.run summary{cursor:pointer;padding:12px;background:#15223a;list-style-position:inside}.run summary:hover{background:#1b2c4a}                .run-body{height:240px;padding:10px;overflow:auto}.history-layout .run-body{height:360px}.run-body.detail-open{height:760px}.run-table{min-width:900px;font-size:12px}.run-table th{position:sticky;top:0;background:#101a2d;z-index:1}.dataset-link{border:0;background:transparent;color:#cfe0ff;text-align:left;padding:0;cursor:pointer}.dataset-link:hover{text-decoration:underline}.inline-detail-row td{padding:0;border-bottom:1px solid #344663}.inline-detail{height:520px;margin:0;padding:14px;background:#0f1a2e;border:1px solid #344663;border-radius:10px;overflow:auto}.inline-detail h3{margin:8px 0;color:#d9e6ff}.inline-detail-head{position:sticky;top:-14px;background:#0f1a2e;padding-bottom:4px;z-index:2}.detail-table{height:560px;overflow:auto}.detail-table.empty{height:auto;overflow:visible}.detail-table table{min-width:1800px}.detail-table th{position:sticky;top:0;background:#131f35;z-index:1;white-space:nowrap}.detail-table td{white-space:nowrap;padding:5px 8px;line-height:1.15}.detail-tabs{display:flex;gap:8px;align-items:center;margin:10px 0;flex-wrap:wrap}.detail-tabs button{border:1px solid #344663;background:#15223a;color:#cfe0ff;border-radius:8px;padding:7px 10px;cursor:pointer}.detail-tabs input{border:1px solid #344663;background:#101a2d;color:#cfe0ff;border-radius:8px;padding:8px;min-width:220px}@media(min-width:1600px){.shell{max-width:1720px}}@media(max-width:850px){.cards{grid-template-columns:1fr 1fr}.layout{display:block}.panel{margin-bottom:18px}}@media(max-width:520px){.shell{padding:16px}.cards{grid-template-columns:1fr 1fr}header{align-items:flex-start;gap:12px}.refresh{font-size:11px}}
        </style></head>
        <body><main class="shell"><header><div class="brand"><div class="logo">DB</div><div><h1>DataBeacon</h1><div class="muted">Dataset freshness & SLA control room</div></div></div>
        <div class="toolbar"><input id="seedRows" type="number" min="0" max="1000000" value="12" title="Generated rows"><button onclick="generate()">Generate & run</button><button onclick="document.getElementById('producerFile').click()">Upload CSV</button><input id="producerFile" type="file" accept=".csv,text/csv" hidden onchange="upload(this)"><button class="refresh" onclick="load(true)">↻ Refresh data</button></div></header><div id="app"><div class="panel">Loading monitoring data...</div></div></main>
        <script>
        let loadSequence=0;
        let detailRequestSequence=0;
        async function load(force=false){if(detailRunId&&!force)return;const sequence=++loadSequence;try{const [d,r]=await Promise.all([fetch('/api/dashboard').then(x=>x.json()),fetch('/api/report').then(x=>x.json())]);if(sequence!==loadSequence)return;
          const statusFilter=document.getElementById('resultStatus')?.value||'all';
          const datasetFilter=document.getElementById('resultDataset')?.value||'all';
          const searchFilter=(document.getElementById('resultSearch')?.value||'').toLowerCase();
          const fromDate=document.getElementById('resultFrom')?.value||'';
          const toDate=document.getElementById('resultTo')?.value||'';
          const filteredDatasets=d.datasets.filter(x=>(statusFilter==='all'||x.sla_status===statusFilter)&&(datasetFilter==='all'||x.dataset_id===datasetFilter)&&(!searchFilter||`${x.dataset_id} ${x.dataset_name}`.toLowerCase().includes(searchFilter))&&(!fromDate||x.expected_at?.slice(0,10)>=fromDate)&&(!toDate||x.expected_at?.slice(0,10)<=toDate));
          const datasetOptions=['<option value="all">All datasets</option>',...d.datasets.map(x=>`<option value="${x.dataset_id}" ${datasetFilter===x.dataset_id?'selected':''}>${x.dataset_name}</option>`)].join('');
          const statusOptions=['all','missing','late','incomplete','fresh','scheduled'].map(x=>`<option value="${x}" ${statusFilter===x?'selected':''}>${x==='all'?'All statuses':x}</option>`).join('');
          const ds=filteredDatasets.map(x=>`<tr><td><b>${x.dataset_name}</b><div class="muted">${x.dataset_id}</div></td><td><span class="pill ${x.sla_status==='arrived'?'scheduled':x.sla_status}">${x.sla_status}</span></td><td>${x.expected_at||'-'}</td><td>${x.completed_at||'-'}</td></tr>`).join('');
          const al=d.alerts.map(x=>`<div class="alert"><div><b class="${x.severity}">${x.severity} · ${x.alert_type}</b><div>${x.message}</div><div class="action">${x.action}</div></div>${x.alert_id?`<button class="ack" onclick="ack(${x.alert_id})">Acknowledge</button>`:''}</div>`).join('');
          const summary=d.summary||{};
          const latest=d.latest_run||{};
          const openRuns=new Set([...document.querySelectorAll('.run[open]')].map(x=>x.dataset.runId));
          const runHistory=d.runs.map((run,index)=>{const rows=(run.datasets||[]).map(x=>{const row=`<tr><td><button class="dataset-link" onclick="showDataset('${run.run_id}','${x.dataset_id}')"><b>${x.dataset_name}</b><div class="muted">${x.dataset_id}</div></button></td><td><span class="pill ${x.sla_status}">${x.sla_status}</span></td><td>${x.event_count||0}</td><td>${x.expected_at||'-'}</td><td>${x.completed_at||'-'}</td></tr>`;const detail=detailRunId===run.run_id&&detailDatasetId===x.dataset_id?`<tr class="inline-detail-row"><td colspan="5"><div class="inline-detail" data-detail-run="${run.run_id}"></div></td></tr>`:'';return row+detail}).join('');return `<details class="run" data-run-id="${run.run_id}" ${index===0&&!openRuns.size?'open':''}><summary><b>${run.started_at||'-'}</b> · ${run.status} · ${Number(run.input_count||0).toLocaleString()} input rows · ${Number(run.accepted_count||0).toLocaleString()} accepted</summary><div class="run-body"><table class="run-table"><tr><th>Dataset (click for details)</th><th>Status</th><th>Records</th><th>First expected</th><th>Last completed</th></tr>${rows}</table></div></details>`}).join('');
          document.getElementById('app').innerHTML=`<section class="cards"><div class="card"><div class="metric-label">Datasets monitored</div><div class="metric">${summary.datasets_monitored||0}</div><div class="muted">${summary.datasets_healthy||0} currently healthy</div></div><div class="card"><div class="metric-label">Latest run rows</div><div class="metric">${(latest.input_count||0).toLocaleString()}</div><div class="muted">${latest.status||'no run'} · ${(latest.accepted_count||0).toLocaleString()} accepted</div></div><div class="card"><div class="metric-label">Platform health</div><div class="metric">${summary.health_percent||0}%</div><div class="healthbar"><div class="healthfill" style="width:${summary.health_percent||0}%"></div></div><div class="muted">${summary.datasets_needing_attention||0} datasets need attention</div></div><div class="card"><div class="metric-label">Open alerts</div><div class="metric">${summary.open_alerts||0}</div><div class="muted">${(r.published_events||0).toLocaleString()} cumulative events</div></div></section><div class="layout"><section class="panel"><div class="panel-head"><div><h2>Dataset health <span class="muted">(current UTC window)</span></h2><div class="muted">Live status for today’s expected arrival window. It may differ from historical run results below.</div></div><div class="legend"><span><i class="dot" style="background:#63e6be"></i>Healthy</span><span><i class="dot" style="background:#ffd27a"></i>Warning</span><span><i class="dot" style="background:#ff91a9"></i>Critical</span></div></div><div class="filters"><label>Status <select id="resultStatus" onchange="load()">${statusOptions}</select></label><label>Dataset <select id="resultDataset" onchange="load()">${datasetOptions}</select></label><label>Search <input id="resultSearch" type="search" value="${searchFilter}" placeholder="ID or name" onkeydown="if(event.key==='Enter')load()"></label><label>From <input id="resultFrom" type="date" value="${fromDate}" onchange="load()"></label><label>To <input id="resultTo" type="date" value="${toDate}" onchange="load()"></label><button onclick="clearFilters()">Clear</button><button onclick="downloadFiltered()">Download filtered CSV</button><span class="muted">${filteredDatasets.length} result(s)</span></div><div class="table-scroll"><table><tr><th>Dataset</th><th>Live status</th><th>Current expected</th><th>Current completed</th></tr>${ds}</table></div></section><section class="panel"><div class="panel-head"><div><h2>Action center</h2><div class="muted">Unacknowledged run and scheduled-window alerts</div></div></div><div class="alerts-scroll">${al||'<div class="muted">No open alerts.</div>'}</div></section></div><div class="history-layout"><section class="panel history-panel"><h2>Pipeline run history</h2><div class="muted">Historical snapshot for each run. One row per dataset per run; expand a dataset for all records.</div>${runHistory||'<div class="muted">No pipeline runs yet.</div>'}</section><section class="panel quality-panel"><h2>Data quality pulse</h2><div class="muted">Published + quarantined records remain traceable</div><pre>${JSON.stringify({latest_run:latest,status_counts:r.status_counts,rejects:r.rejects,alerts:r.alerts},null,2)}</pre></section></div><section id="datasetDetails" class="panel" hidden><h2 id="detailTitle">Dataset details</h2><div id="detailSummary" class="muted"></div><div class="detail-tabs"><input id="detailSearch" type="search" placeholder="Search all columns" onkeydown="if(event.key==='Enter')refreshDetails()"><button onclick="refreshDetails()">Search</button><button onclick="closeDetails()">Close</button></div><div id="detailContent"></div></section>`;
          openRuns.forEach(runId=>{const run=document.querySelector(`.run[data-run-id="${runId}"]`);if(run)run.open=true});
          if(detailRunId&&detailDatasetId){const host=detailHost();if(host){host.hidden=false;host.closest('.run-body')?.classList.add('detail-open');refreshDetails()}}
        }catch(e){document.getElementById('app').innerHTML='<div class="panel critical">Unable to load monitoring data. Check the API and database.</div>'}}
        async function ack(id){await fetch('/api/alerts/'+id+'/acknowledge',{method:'POST'});load(true)}
        function download(kind){window.location.href='/api/download/'+kind}
        function clearFilters(){['resultStatus','resultDataset','resultSearch','resultFrom','resultTo'].forEach(id=>{const element=document.getElementById(id);if(element)element.value=id==='resultStatus'||id==='resultDataset'?'all':''});load(true)}
        function downloadFiltered(){const status=document.getElementById('resultStatus')?.value||'all';const dataset=document.getElementById('resultDataset')?.value||'all';const params=new URLSearchParams();const kind=status==='missing'?'windows':'events';if(status!=='all')params.set('status',status);if(dataset!=='all')params.set('dataset_id',dataset);const search=document.getElementById('resultSearch')?.value||'';const from=document.getElementById('resultFrom')?.value||'';const to=document.getElementById('resultTo')?.value||'';if(search)params.set('search',search);if(from)params.set('from_date',from);if(to)params.set('to_date',to);window.location.href='/api/download/'+kind+'?'+params.toString()}
        let detailRunId='',detailDatasetId='',detailSearchValue='',detailScrollTop=0;
        function detailHost(){return document.querySelector(`.inline-detail[data-detail-run="${detailRunId}"]`)}
        async function showDataset(runId,datasetId){detailRunId=runId;detailDatasetId=datasetId;await load(true)}
        async function refreshDetails(){const host=detailHost();if(!host)return;const sequence=++detailRequestSequence;detailScrollTop=host.scrollTop;const search=detailSearchValue;const data=await fetch('/api/runs/'+encodeURIComponent(detailRunId)+'/datasets/'+encodeURIComponent(detailDatasetId)+'?search='+encodeURIComponent(search)).then(x=>x.json());if(sequence!==detailRequestSequence)return;const accepted=(data.accepted||[]).map((x,index)=>`<tr><td>${index+1}</td><td>${x.observation_id}</td><td>${x.event_key}</td><td>${x.run_id}</td><td>${x.dataset_id}</td><td>${x.source_file}</td><td>${x.expected_at}</td><td>${x.arrived_at||'-'}</td><td>${x.completed_at||'-'}</td><td>${x.row_count}</td><td>${x.freshness_minutes??'-'}</td><td>${x.completion_minutes??'-'}</td><td>${x.sla_status}</td><td>${x.observed_at}</td><td>${x.raw_payload}</td></tr>`).join('')||'<tr><td colspan="15" class="muted">No accepted records.</td></tr>';const rejects=(data.rejects||[]).map((x,index)=>`<tr><td>${index+1}</td><td>${x.reject_id}</td><td>${x.source_file}</td><td>${x.source_row}</td><td>${x.reason_code}</td><td>${x.reason_detail}</td><td>${x.raw_payload}</td></tr>`).join('')||'<tr><td colspan="7" class="muted">No rejected records.</td></tr>';const duplicates=(data.duplicates||[]).map((x,index)=>`<tr><td>${index+1}</td><td>${x.duplicate_id}</td><td>${x.run_id}</td><td>${x.dataset_id}</td><td>${x.event_key}</td><td>${x.source_file}</td><td>${x.source_row}</td><td>${x.reason}</td></tr>`).join('')||'<tr><td colspan="8" class="muted">No duplicate records.</td></tr>';const inputs=(labels)=>labels.map(label=>`<th>${label}</th>`).join('');host.innerHTML=`<div class="inline-detail-head"><h3>${data.dataset_name} · ${data.dataset_id}</h3><div class="muted">Run ${data.run.run_id} | Accepted: ${data.counts.accepted} | Rejected: ${data.counts.rejected} | Duplicates: ${data.counts.duplicates} | Arrival/SLA telemetry only</div><div class="detail-tabs"><input class="detail-search" type="search" value="${detailSearchValue}" placeholder="Search all columns" oninput="detailSearchValue=this.value" onkeydown="if(event.key==='Enter')refreshDetails()"><button onclick="refreshDetails()">Search</button><button onclick="downloadDetails('accepted')">Download accepted</button><button onclick="downloadDetails('rejected')">Download rejected</button><button onclick="downloadDetails('duplicates')">Download duplicates</button><button onclick="closeDetails()">Close</button></div></div><h3>Accepted data / observations</h3><div class="detail-table${(data.accepted||[]).length?"":" empty"}"><table id="acceptedDetailTable"><thead><tr>${inputs(['S.No','Observation ID','Event ID','Run ID','Dataset ID','Source file','Expected','Arrived','Completed','Row count','Freshness min','Completion min','SLA status','Observed','Raw payload'])}</tr></thead><tbody>${accepted}</tbody></table></div><h3>Rejected source rows</h3><div class="detail-table${(data.rejects||[]).length?"":" empty"}"><table id="rejectedDetailTable"><thead><tr>${inputs(['S.No','Reject ID','Source file','Source row','Reason code','Reason details','Raw payload'])}</tr></thead><tbody>${rejects}</tbody></table></div><h3>Duplicate event records</h3><div class="detail-table${(data.duplicates||[]).length?"":" empty"}"><table id="duplicatesDetailTable"><thead><tr>${inputs(['S.No','Duplicate ID','Run ID','Dataset ID','Event ID','Source file','Source row','Reason'])}</tr></thead><tbody>${duplicates}</tbody></table></div>`;host.scrollTop=detailScrollTop}
        function filterDetailTable(id){const table=document.getElementById(id);if(!table)return;const filters=[...table.querySelectorAll('thead input')].map(x=>x.value.toLowerCase());[...table.tBodies[0].rows].forEach(row=>{row.style.display=filters.every((value,index)=>!value||row.cells[index].textContent.toLowerCase().includes(value))?'':'none'})}
        function closeDetails(){const host=detailHost();host?.closest('.run-body')?.classList.remove('detail-open');detailRunId='';detailDatasetId='';detailSearchValue='';detailScrollTop=0;load(true)}
        function downloadDetails(kind){window.location.href='/api/download/run-dataset/'+encodeURIComponent(detailRunId)+'/'+encodeURIComponent(detailDatasetId)+'/'+kind}
        async function generate(){const rows=Number(document.getElementById('seedRows').value||12);const response=await fetch('/api/runs/generate?rows='+encodeURIComponent(rows)+'&randomize_status=true',{method:'POST'});if(!response.ok){alert(await response.text());return}load(true)}
        async function upload(input){const file=input.files[0];if(!file)return;const response=await fetch('/api/runs/upload',{method:'POST',headers:{'Content-Type':'text/csv'},body:await file.arrayBuffer()});if(!response.ok){alert(await response.text());return}input.value='';load(true)}
        load(); setInterval(()=>load(false),5000);</script></body></html>"""

    return app


async def _monitor_loop(config: Settings) -> None:
    while True:
        try:
            await asyncio.to_thread(
                monitor_expected_windows,
                config.db_path,
                None,
                config.arrival_grace_minutes,
            )
        except (sqlite3.Error, OSError):
            logger.exception("sla_monitor_background_iteration_failed")
        await asyncio.sleep(config.monitor_interval_seconds)


app = create_app()
