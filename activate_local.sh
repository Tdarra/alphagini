#!/usr/bin/env bash
# Activate local venv and ensure local packages import (services.api.*)
source "$(dirname "$0")/.venv_local/bin/activate"
export PYTHONPATH="$(dirname "$0")"
echo "Activated .venv_local"
echo "PYTHONPATH=$PYTHONPATH"
python --version
