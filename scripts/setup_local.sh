#!/usr/bin/env bash
set -euo pipefail

# Resolve repo root (script expected at repo_root/scripts/setup_local.sh)
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON:-python3}"
VENV_DIR="$REPO_ROOT/.venv_local"

echo "Repo: $REPO_ROOT"
echo "Python: $PYTHON_BIN"
echo "Venv: $VENV_DIR"

# Ensure packages are importable as modules: services/, services/api/
mkdir -p "$REPO_ROOT/services/api"
touch "$REPO_ROOT/services/__init__.py" "$REPO_ROOT/services/api/__init__.py"

# Create & activate venv
$PYTHON_BIN -m venv "$VENV_DIR"
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

python -m pip install --upgrade pip setuptools wheel

# Write local requirements (only if not present)
mkdir -p "$REPO_ROOT/tools"
REQ_FILE="$REPO_ROOT/tools/requirements-local.txt"
if [[ ! -f "$REQ_FILE" ]]; then
  cat > "$REQ_FILE" << 'EOF'
# ---- core numeric / data ----
pandas==2.2.2
numpy==1.26.4
python-dateutil>=2.8.2
pytz>=2023.3
# optional: local training helpers (if you call them from services/api)
statsmodels==0.14.2

# ---- match imports used by services/api (import compatibility only) ----
fastapi==0.111.0
uvicorn==0.30.3
pydantic==2.7.4

# If services/api/app.py imports BigQuery/db-dtypes even when unused locally:
google-cloud-bigquery==3.25.0
db-dtypes==1.2.0
google-auth>=2.30.0
EOF
  echo "Wrote $REQ_FILE"
fi

pip install -r "$REQ_FILE"

# Convenience activator that also sets PYTHONPATH to repo root
ACT="$REPO_ROOT/activate_local.sh"
cat > "$ACT" << 'EOF'
#!/usr/bin/env bash
# Activate local venv and ensure local packages import (services.api.*)
source "$(dirname "$0")/.venv_local/bin/activate"
export PYTHONPATH="$(dirname "$0")"
echo "Activated .venv_local"
echo "PYTHONPATH=$PYTHONPATH"
python --version
EOF
chmod +x "$ACT"

echo
echo "✅ Local environment ready."
echo "Next steps:"
echo "  1) source ./activate_local.sh"
echo "  2) python tools/local_cli.py --help"

