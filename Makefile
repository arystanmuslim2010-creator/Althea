SHELL := powershell.exe

.PHONY: dev backend frontend worker event-worker streaming-worker test lint

dev:
	powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\dev-stack.ps1

backend:
	cd backend; python -m uvicorn main:app --reload --port 8000

frontend:
	cd frontend; npm run dev

worker:
	cd backend; python -m workers.pipeline_worker

event-worker:
	cd backend; python -m workers.event_worker

streaming-worker:
	cd backend; python -m workers.streaming_worker

test:
	cd backend; pytest -q
	cd frontend; npm run test -- --run

lint:
	cd backend; python -m compileall . -x "(\\.venv|__pycache__)"
	cd frontend; npm run build
