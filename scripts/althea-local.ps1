param(
    [ValidateSet('api','worker','event','streaming','all-workers','clear-queue','bootstrap-model','reset-stream')]
    [string]$Role = 'api',
    [string]$Tenant = 'default-bank',
    [string]$DbUrl = 'postgresql+psycopg://althea:althea_dev_password@localhost:5432/althea',
    [string]$RedisUrl = 'redis://localhost:6379/0',
    [string]$Queue = 'althea-pipeline',
    [string]$JwtSecret = 'replace-with-strong-secret',
    [int]$Port = 8000
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$backend = Join-Path $repoRoot 'backend'
$py = Join-Path $backend '.venv\Scripts\python.exe'

if (!(Test-Path $py)) {
    Write-Error "Python venv not found at $py. Create it first: cd $backend; python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt"
    exit 1
}

$env:ALTHEA_ENV = 'development'
$env:ALTHEA_DATABASE_URL = $DbUrl
$env:ALTHEA_REDIS_URL = $RedisUrl
$env:ALTHEA_QUEUE_MODE = 'rq'
$env:ALTHEA_RQ_QUEUE = $Queue
$env:ALTHEA_DEFAULT_TENANT_ID = $Tenant
$env:ALTHEA_JWT_SECRET = $JwtSecret
$env:PYTHONUNBUFFERED = '1'

Push-Location $backend
try {
    switch ($Role) {
        'api' {
            & $py -m uvicorn main:app --host 127.0.0.1 --port $Port
        }
        'worker' {
            & $py -m workers.pipeline_worker
        }
        'event' {
            & $py -m workers.event_worker
        }
        'streaming' {
            & $py -m workers.streaming_worker
        }
        'all-workers' {
            & $py -m workers.all_in_one_worker
        }
        'clear-queue' {
            @'
import os
import redis
from rq import Queue
r = redis.Redis.from_url(os.environ["ALTHEA_REDIS_URL"])
q = Queue(os.environ["ALTHEA_RQ_QUEUE"], connection=r)
print("queue before:", q.count)
q.empty()
print("queue after:", q.count)
'@ | & $py -
        }
        'bootstrap-model' {
            @'
import hashlib
import io
import numpy as np
import pandas as pd
from joblib import dump as joblib_dump
from sklearn.ensemble import RandomForestClassifier

from core.dependencies import get_feature_service, get_model_registry
from core.config import get_settings

settings = get_settings()
tenant = settings.default_tenant_id
rng = np.random.default_rng(42)
n = 2500
source = pd.DataFrame({
    'alert_id': [f'BOOT{i:06d}' for i in range(n)],
    'user_id': [f'U{i%400:05d}' for i in range(n)],
    'amount': np.abs(rng.normal(1100, 450, n)),
    'txn_count_24h': rng.integers(1, 16, n),
    'country_risk': rng.uniform(0, 1, n),
    'segment': rng.choice(['retail','corporate','private'], n),
    'typology': rng.choice(['structuring','rapid_movement','sanctions'], n),
})

feature_service = get_feature_service()
bundle = feature_service.generate_training_features(source)
X = bundle['feature_matrix']
signal = ((source['amount'] > source['amount'].quantile(0.8)).astype(int) + (source['txn_count_24h'] > 10).astype(int) + (source['country_risk'] > 0.75).astype(int))
y = (signal >= 2).astype(int)
if y.nunique() < 2:
    y = pd.Series(rng.integers(0, 2, len(X)))

model = RandomForestClassifier(n_estimators=120, random_state=42)
model.fit(X, y)
buf = io.BytesIO()
joblib_dump(model, buf)

schema = bundle.get('feature_schema') or {'columns': [{'name': c, 'dtype': str(X[c].dtype)} for c in X.columns], 'schema_hash': ''}
if not schema.get('schema_hash'):
    schema['schema_hash'] = hashlib.sha256('|'.join(X.columns).encode('utf-8')).hexdigest()

registry = get_model_registry()
rec = registry.register_model(
    tenant_id=tenant,
    artifact_bytes=buf.getvalue(),
    training_dataset_hash=hashlib.sha256(pd.util.hash_pandas_object(source, index=True).values.tobytes()).hexdigest(),
    feature_schema=schema,
    metrics={'train_rows': int(len(X)), 'features': int(X.shape[1]), 'positive_rate': float(np.mean(y))},
    training_metadata={'artifact_format': 'joblib', 'is_active': True, 'bootstrap': True, 'feature_schema_version': 'v1'},
    approval_status='approved',
    approved_by='dev-script',
)
print('model_version:', rec['model_version'])
'@ | & $py -
        }
        'reset-stream' {
            @'
from core.dependencies import get_streaming_backbone
from events.streaming.topics import ALERTS_INGESTED, ALERTS_FEATURES_GENERATED, ALERTS_SCORED, ALERTS_PRIORITIZED

stream = get_streaming_backbone()
consumer = "streaming-worker"
for topic in [ALERTS_INGESTED, ALERTS_FEATURES_GENERATED, ALERTS_SCORED, ALERTS_PRIORITIZED]:
    latest = stream.latest_event_id(topic) or "0-0"
    stream.set_cursor(topic=topic, event_id=latest, consumer=consumer)
    print(f"{topic}: cursor={latest}")
'@ | & $py -
        }
    }
}
finally {
    Pop-Location
}
