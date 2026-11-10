"""Run pytest while avoiding a machine-level py.py shadowing module."""

import sys


def main() -> int:
    sys.path[:] = [
        path for path in sys.path
        if path.lower() != r"c:\users\sushm\appdata\local\programs\python\python311"
    ]
    import pytest

    return pytest.main(["-q"])


if __name__ == "__main__":
    raise SystemExit(main())
