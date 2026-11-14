import os
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parents[1] / "dbt" / "nyc_mobility"


@pytest.mark.dbt
def test_dbt_parse_succeeds():
    env = {**os.environ, "GCP_PROJECT_ID": "local-parse", "DBT_PROFILES_DIR": str(PROJECT)}
    subprocess.run([sys.executable, "-m", "dbt.cli.main", "deps"], cwd=PROJECT, env=env, capture_output=True, text=True)
    r = subprocess.run([sys.executable, "-m", "dbt.cli.main", "parse", "--no-partial-parse"], cwd=PROJECT, env=env, capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
