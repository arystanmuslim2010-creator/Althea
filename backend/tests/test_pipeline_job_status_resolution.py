from __future__ import annotations

from types import SimpleNamespace

from services.pipeline_service import PipelineService


class _FakeQueue:
    def __init__(self, status_payload: dict | None = None) -> None:
        self._status_payload = status_payload

    def queue_depth(self, queue_name: str) -> int:
        return 0

    def get_status(self, job_id: str) -> dict | None:
        return self._status_payload

    def set_status(self, job_id: str, payload: dict) -> None:
        self._status_payload = payload

    def enqueue(self, **kwargs) -> None:
        return None


class _FakeRepo:
    def __init__(self, job_payload: dict | None = None) -> None:
        self._job_payload = job_payload

    def get_pipeline_job(self, tenant_id: str, job_id: str) -> dict | None:
        return self._job_payload


def _service(repo: _FakeRepo, queue: _FakeQueue) -> PipelineService:
    settings = SimpleNamespace(
        rq_queue_name="althea-pipeline",
        queue_mode="rq",
        redis_url="redis://localhost:6379/0",
        rq_job_timeout_seconds=900,
    )
    return PipelineService(
        settings=settings,
        repository=repo,
        event_bus=SimpleNamespace(),
        job_queue=queue,
        ingestion_service=SimpleNamespace(),
        feature_service=SimpleNamespace(),
        inference_service=SimpleNamespace(),
        governance_service=SimpleNamespace(),
        model_monitoring_service=SimpleNamespace(),
        streaming_orchestrator=None,
    )


def test_get_job_status_ignores_stale_discarded_cache_when_db_has_job() -> None:
    repo = _FakeRepo(job_payload={"job_id": "job_1", "status": "running"})
    queue = _FakeQueue(status_payload={"job_id": "job_1", "status": "discarded", "detail": "stale"})
    service = _service(repo=repo, queue=queue)

    status = service.get_job_status(tenant_id="tenant-a", job_id="job_1")
    assert status["status"] == "running"


def test_get_job_status_uses_cached_terminal_progress_when_db_is_queued() -> None:
    repo = _FakeRepo(job_payload={"job_id": "job_2", "status": "queued"})
    queue = _FakeQueue(status_payload={"job_id": "job_2", "status": "completed", "alerts": 400})
    service = _service(repo=repo, queue=queue)

    status = service.get_job_status(tenant_id="tenant-a", job_id="job_2")
    assert status["status"] == "completed"
    assert status["alerts"] == 400
