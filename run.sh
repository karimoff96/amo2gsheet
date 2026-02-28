#!/bin/bash
cd "$(dirname "$0")"
venv/bin/python -m uvicorn sync_service:app --host 0.0.0.0 --port 8000
