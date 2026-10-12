#!/usr/bin/env bash
# Start the app in SAP Business Application Studio (or any Linux shell).
# Usage: bash run.sh            -> http://localhost:8000/docs
set -e
cd "$(dirname "$0")"
PY=$(command -v python3 || command -v python) || { echo "Python not found in this dev space"; exit 1; }
if [ ! -x .venv/bin/python ] || ! .venv/bin/python -c "" 2>/dev/null; then
  echo "Using virtual environment .venv-bas"
  VENV=.venv-bas
else
  VENV=.venv
fi
[ -x "$VENV/bin/python" ] || "$PY" -m venv "$VENV"
"$VENV/bin/python" -m pip install -q -r requirements.txt
echo "Starting app on port ${PORT:-8000} - open /docs to test"
exec "$VENV/bin/python" -m uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
