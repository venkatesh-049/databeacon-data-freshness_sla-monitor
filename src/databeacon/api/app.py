from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

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
from databeacon.settings import Settings


def create_app(settings: Settings | None = None) -> FastAPI:
    config = settings or Settings()
    config.ensure_directories()
    app = FastAPI(title="DataBeacon")

    @app.get("/api/health")
    def health() -> dict:
        connection = connect(config.db_path)
        connection.close()
        return {"status": "ok"}

    @app.get("/api/dashboard")
    def dashboard() -> dict:
        connection = connect(config.db_path)
        seed_datasets(connection)
        data = dashboard_data(connection)
        connection.close()
        return data

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
        .refresh{border:1px solid #344663;background:#15223a;color:#cfe0ff;border-radius:10px;padding:10px 14px;cursor:pointer}.refresh:hover{background:#203354}
        .cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:18px}.card,.panel{background:linear-gradient(145deg,#131f35,#101a2d);border:1px solid #263957;border-radius:16px;padding:18px;box-shadow:0 10px 30px #00000022}
        .card{position:relative;overflow:hidden}.card:after{content:"";position:absolute;width:70px;height:70px;border-radius:50%;right:-25px;top:-28px;background:#4e7cff22}.metric{font-size:32px;font-weight:800;margin:9px 0 2px}.metric-label{color:#8ea0be;font-size:12px;text-transform:uppercase;letter-spacing:.7px}
        .layout{display:grid;grid-template-columns:minmax(0,1.55fr) minmax(0,1fr);gap:18px;margin-bottom:18px}.panel-head{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:12px}
        table{border-collapse:collapse;width:100%;font-size:13px}th{text-align:left;color:#8294b5;font-weight:600;border-bottom:1px solid #263957;padding:10px 8px}
        td{border-bottom:1px solid #1e2d46;padding:13px 8px}tr:last-child td{border-bottom:0}.pill{display:inline-block;border-radius:20px;padding:5px 9px;font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:.5px}
        .fresh{background:#143d3b;color:#63e6be}.late{background:#48351b;color:#ffd27a}.missing{background:#4b202e;color:#ff91a9}.incomplete{background:#48351b;color:#ffd27a}.scheduled{background:#21304a;color:#a8b8d5}
        .critical{color:#ff91a9}.warning{color:#ffd27a}.alert{display:flex;justify-content:space-between;gap:12px;padding:13px 0;border-bottom:1px solid #1e2d46}
        .alert:last-child{border:0}.alert b{font-size:10px;text-transform:uppercase;letter-spacing:.5px}.action{font-size:12px;color:#8ea0be;margin-top:4px;line-height:1.4}.ack{border:0;background:#223a6c;color:#bcd2ff;border-radius:7px;padding:6px 9px;cursor:pointer;white-space:nowrap}
        .healthbar{height:7px;background:#23324b;border-radius:9px;overflow:hidden;margin-top:12px}.healthfill{height:100%;background:linear-gradient(90deg,#63e6be,#4e7cff);border-radius:9px}.legend{display:flex;gap:12px;margin-top:12px;font-size:11px;color:#93a3bf}.dot{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:4px}
        pre{font-size:11px;white-space:pre-wrap;color:#a7b6d0;max-height:180px;overflow:auto}.run{border:1px solid #263957;border-radius:10px;margin-top:10px;overflow:hidden}.run summary{cursor:pointer;padding:12px;background:#15223a;list-style-position:inside}.run summary:hover{background:#1b2c4a}.run-body{padding:10px;overflow:auto}.run-table{min-width:620px;font-size:12px}@media(min-width:1600px){.shell{max-width:1720px}}@media(max-width:850px){.cards{grid-template-columns:1fr 1fr}.layout{display:block}.panel{margin-bottom:18px}}@media(max-width:520px){.shell{padding:16px}.cards{grid-template-columns:1fr 1fr}header{align-items:flex-start;gap:12px}.refresh{font-size:11px}}
        </style></head>
        <body><main class="shell"><header><div class="brand"><div class="logo">DB</div><div><h1>DataBeacon</h1><div class="muted">Dataset freshness & SLA control room</div></div></div>
        <button class="refresh" onclick="load()">↻ Refresh data</button></header><div id="app"><div class="panel">Loading monitoring data...</div></div></main>
        <script>
        async function load(){try{const [d,r]=await Promise.all([fetch('/api/dashboard').then(x=>x.json()),fetch('/api/report').then(x=>x.json())]);
          const ds=d.datasets.map(x=>`<tr><td><b>${x.dataset_name}</b><div class="muted">${x.dataset_id}</div></td><td><span class="pill ${x.sla_status}">${x.sla_status}</span></td><td>${x.expected_at||'-'}</td><td>${x.completed_at||'-'}</td></tr>`).join('');
          const al=d.alerts.map(x=>`<div class="alert"><div><b class="${x.severity}">${x.severity} · ${x.alert_type}</b><div>${x.message}</div><div class="action">${x.action}</div></div>${x.alert_id?`<button class="ack" onclick="ack(${x.alert_id})">Acknowledge</button>`:''}</div>`).join('');
          const summary=d.summary||{};
          const latest=d.latest_run||{};
          const openRuns=new Set([...document.querySelectorAll('.run[open]')].map(x=>x.dataset.runId));
          const runHistory=d.runs.map((run,index)=>{const rows=(run.datasets||[]).map(x=>`<tr><td><b>${x.dataset_name}</b><div class="muted">${x.dataset_id}</div></td><td><span class="pill ${x.sla_status}">${x.sla_status}</span></td><td>${x.expected_at||'-'}</td><td>${x.completed_at||'-'}</td></tr>`).join('');return `<details class="run" data-run-id="${run.run_id}" ${index===0&&!openRuns.size?'open':''}><summary><b>${run.started_at||'-'}</b> · ${run.status} · ${Number(run.input_count||0).toLocaleString()} input rows · ${Number(run.accepted_count||0).toLocaleString()} accepted</summary><div class="run-body"><table class="run-table"><tr><th>Dataset</th><th>Status</th><th>Expected</th><th>Completed</th></tr>${rows}</table></div></details>`}).join('');
          document.getElementById('app').innerHTML=`<section class="cards"><div class="card"><div class="metric-label">Datasets monitored</div><div class="metric">${summary.datasets_monitored||0}</div><div class="muted">${summary.datasets_healthy||0} currently healthy</div></div><div class="card"><div class="metric-label">Latest run rows</div><div class="metric">${(latest.input_count||0).toLocaleString()}</div><div class="muted">${latest.status||'no run'} · ${(latest.accepted_count||0).toLocaleString()} accepted</div></div><div class="card"><div class="metric-label">Platform health</div><div class="metric">${summary.health_percent||0}%</div><div class="healthbar"><div class="healthfill" style="width:${summary.health_percent||0}%"></div></div><div class="muted">${summary.datasets_needing_attention||0} datasets need attention</div></div><div class="card"><div class="metric-label">Open alerts</div><div class="metric">${summary.open_alerts||0}</div><div class="muted">${(r.published_events||0).toLocaleString()} cumulative events</div></div></section><div class="layout"><section class="panel"><div class="panel-head"><div><h2>Dataset health</h2><div class="muted">Status calculated from the latest pipeline run</div></div><div class="legend"><span><i class="dot" style="background:#63e6be"></i>Healthy</span><span><i class="dot" style="background:#ffd27a"></i>Warning</span><span><i class="dot" style="background:#ff91a9"></i>Critical</span></div></div><table><tr><th>Dataset</th><th>Status</th><th>Expected</th><th>Completed</th></tr>${ds}</table></section><section class="panel"><div class="panel-head"><div><h2>Action center</h2><div class="muted">Alerts from latest run</div></div></div>${al||'<div class="muted">All clear for the latest run.</div>'}</section></div><div class="layout"><section class="panel"><h2>Pipeline run history</h2><div class="muted">Expand any run to inspect dataset health for that run</div>${runHistory||'<div class="muted">No pipeline runs yet.</div>'}</section><section class="panel"><h2>Data quality pulse</h2><div class="muted">Published + quarantined records remain traceable</div><pre>${JSON.stringify({latest_run:latest,status_counts:r.status_counts,rejects:r.rejects,alerts:r.alerts},null,2)}</pre></section></div>`;
          openRuns.forEach(runId=>{const run=document.querySelector(`.run[data-run-id="${runId}"]`);if(run)run.open=true});
        }catch(e){document.getElementById('app').innerHTML='<div class="panel critical">Unable to load monitoring data. Check the API and database.</div>'}}
        async function ack(id){await fetch('/api/alerts/'+id+'/acknowledge',{method:'POST'});load()} load(); setInterval(load,5000);</script></body></html>"""

    return app


app = create_app()
