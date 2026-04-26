# Testing

Fast default backend tests exclude slow and benchmark markers:

```bash
python -m pytest backend/tests -m "not slow and not benchmark"
```

Docker backend test run (from repo root):

```bash
docker compose -f docker/docker-compose.dev.yml run --rm backend sh -lc "python -m pip install --user --no-warn-script-location pytest && python -m pytest tests -m 'not slow and not benchmark'"
```

Security and access-control focused tests:

```bash
python -m pytest backend/tests -k "access or auth or admin or governance or pilot_metrics or pilot_data_contract"
```

Benchmark tests are opt-in:

```bash
python -m pytest backend/tests -m "benchmark"
```

Frontend tests:

```bash
npm --prefix frontend run test -- --run
```

The IBM AML, Protocol B, LI transfer, sequence-model, horizon, and graph-heavy tests are marked `benchmark`/`slow` by collection rules so they do not run in the default fast suite.
