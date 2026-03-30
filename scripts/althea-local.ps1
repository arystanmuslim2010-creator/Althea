param(
    [ValidateSet('api','worker','event','streaming','all-workers','clear-queue','bootstrap-model','reset-stream')]
    [string]$Role = 'api',
    [string]$Tenant = 'default-bank',
    [string]$DbUrl = $env:ALTHEA_DATABASE_URL,
    [string]$RedisUrl = $(if ($env:ALTHEA_REDIS_URL) { $env:ALTHEA_REDIS_URL } else { 'redis://localhost:6379/0' }),
    [string]$Queue = 'althea-pipeline',
    [string]$JwtSecret = $env:ALTHEA_JWT_SECRET,
    [int]$Port = 8000
)

function Get-ListeningPid {
    param([int]$LocalPort)
    $conn = Get-NetTCPConnection -LocalPort $LocalPort -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -eq $conn) {
        return $null
    }
    return [int]$conn.OwningProcess
}

function Get-ProcessCommandLine {
    param([int]$Pid)
    try {
        $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $Pid" -ErrorAction Stop
        return [string]($proc.CommandLine)
    }
    catch {
        return ""
    }
}

function Test-AltheaApiHealth {
    param([int]$ApiPort)
    try {
        $res = Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:$ApiPort/health" -TimeoutSec 2
        if ($null -ne $res) {
            return $true
        }
    }
    catch {}
    return $false
}

function Ensure-ApiPortReady {
    param([int]$ApiPort)
    $listenerProcessId = Get-ListeningPid -LocalPort $ApiPort
    if ($null -eq $listenerProcessId) {
        return
    }

    if (Test-AltheaApiHealth -ApiPort $ApiPort) {
        Write-Host "ALTHEA API is already running on port $ApiPort (PID $listenerProcessId). Reusing existing process."
        exit 0
    }

    $processName = ""
    try {
        $processName = (Get-Process -Id $listenerProcessId -ErrorAction Stop).ProcessName
    }
    catch {}
    $commandLine = Get-ProcessCommandLine -Pid $listenerProcessId

    if ($commandLine -match 'uvicorn\s+main:app' -or $commandLine -match 'backend\\main\.py') {
        Write-Warning "Port $ApiPort is occupied by a stale backend process (PID $listenerProcessId). Restarting it."
        Stop-Process -Id $listenerProcessId -Force -ErrorAction Stop
        Start-Sleep -Milliseconds 400
        return
    }

    Write-Error "Port $ApiPort is already in use by PID $listenerProcessId ($processName). Stop that process or run with -Port <other-port>."
    exit 1
}

function Test-IsPlaceholderDbUrl {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $true
    }
    $normalized = $Value.ToLowerInvariant()
    return (
        $normalized.Contains("<password>") -or
        $normalized.Contains("your_password") -or
        $normalized.Contains("your-password")
    )
}

function ConvertTo-DbUriPart {
    param([string]$Value)
    if ($null -eq $Value) { return "" }
    return [System.Uri]::EscapeDataString($Value)
}

function Get-DockerPostgresDbUrl {
    $docker = Get-Command docker -ErrorAction SilentlyContinue
    if ($null -eq $docker) {
        return $null
    }

    $containerName = ""
    try {
        $containerName = (& docker ps --filter "publish=5432" --filter "ancestor=postgres:16" --format "{{.Names}}" | Select-Object -First 1).Trim()
        if ([string]::IsNullOrWhiteSpace($containerName)) {
            $containerName = (& docker ps --filter "publish=5432" --format "{{.Names}}" | Select-Object -First 1).Trim()
        }
    }
    catch {
        return $null
    }
    if ([string]::IsNullOrWhiteSpace($containerName)) {
        return $null
    }

    try {
        $envLines = & docker inspect $containerName --format "{{range .Config.Env}}{{println .}}{{end}}"
    }
    catch {
        return $null
    }

    $pairs = @{}
    foreach ($line in $envLines) {
        if ($line -match '^[^=]+=') {
            $parts = $line -split '=', 2
            $pairs[$parts[0]] = $parts[1]
        }
    }

    $dbUser = if ($pairs.ContainsKey("POSTGRES_USER")) { $pairs["POSTGRES_USER"] } else { "althea" }
    $dbPass = if ($pairs.ContainsKey("POSTGRES_PASSWORD")) { $pairs["POSTGRES_PASSWORD"] } else { "" }
    $dbName = if ($pairs.ContainsKey("POSTGRES_DB")) { $pairs["POSTGRES_DB"] } else { "althea" }
    if ([string]::IsNullOrWhiteSpace($dbPass)) {
        return $null
    }

    $u = ConvertTo-DbUriPart -Value $dbUser
    $p = ConvertTo-DbUriPart -Value $dbPass
    $d = ConvertTo-DbUriPart -Value $dbName
    return ("postgresql+psycopg://{0}:{1}@127.0.0.1:5432/{2}" -f $u, $p, $d)
}

function Test-DbConnection {
    param(
        [string]$PythonExe,
        [string]$DbUrl
    )
    if ([string]::IsNullOrWhiteSpace($DbUrl)) {
        return $false
    }
    $env:ALTHEA_DB_TEST_URL = $DbUrl
    & $PythonExe -c "import os,sys,psycopg; url=os.environ.get('ALTHEA_DB_TEST_URL','').strip().replace('postgresql+psycopg://','postgresql://',1); 
try:
 conn=psycopg.connect(url); conn.close(); print('db-ok')
except Exception as e:
 print(f'db-auth-failed: {e.__class__.__name__}')
 sys.exit(2)"
    return ($LASTEXITCODE -eq 0)
}

function Get-RedactedDbUrl {
    param([string]$DbUrl)
    if ([string]::IsNullOrWhiteSpace($DbUrl)) {
        return ""
    }
    return ($DbUrl -replace '://([^:/]+):([^@]+)@', '://$1:***@')
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$backend = Join-Path $repoRoot 'backend'
$py = Join-Path $backend '.venv\Scripts\python.exe'

if (!(Test-Path $py)) {
    Write-Error "Python venv not found at $py. Create it first: cd $backend; python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt"
    exit 1
}

$env:ALTHEA_ENV = 'development'
$resolvedDbUrl = if (Test-IsPlaceholderDbUrl -Value $DbUrl) { $null } else { $DbUrl }
if ([string]::IsNullOrWhiteSpace($resolvedDbUrl) -and -not (Test-IsPlaceholderDbUrl -Value $env:ALTHEA_DATABASE_URL)) {
    $resolvedDbUrl = $env:ALTHEA_DATABASE_URL
}
if ([string]::IsNullOrWhiteSpace($resolvedDbUrl)) {
    $resolvedDbUrl = Get-DockerPostgresDbUrl
}

$requiresDb = @('api','worker','event','streaming','all-workers','bootstrap-model') -contains $Role
if ($requiresDb) {
    if ([string]::IsNullOrWhiteSpace($resolvedDbUrl)) {
        Write-Error "Cannot resolve database URL. Set -DbUrl explicitly or start docker postgres first."
        exit 1
    }

    if (-not (Test-DbConnection -PythonExe $py -DbUrl $resolvedDbUrl)) {
        $dockerDbUrl = Get-DockerPostgresDbUrl
        if (-not [string]::IsNullOrWhiteSpace($dockerDbUrl) -and $dockerDbUrl -ne $resolvedDbUrl -and (Test-DbConnection -PythonExe $py -DbUrl $dockerDbUrl)) {
            Write-Warning "Provided database URL failed. Falling back to docker postgres credentials."
            $resolvedDbUrl = $dockerDbUrl
        }
        else {
            $masked = Get-RedactedDbUrl -DbUrl $resolvedDbUrl
            Write-Error "Database auth failed for $masked . Fix credentials or reset postgres password, then retry."
            exit 1
        }
    }
}

$env:ALTHEA_DATABASE_URL = $resolvedDbUrl
$env:ALTHEA_REDIS_URL = $RedisUrl
$env:ALTHEA_QUEUE_MODE = 'rq'
$env:ALTHEA_RQ_QUEUE = $Queue
$env:ALTHEA_DEFAULT_TENANT_ID = $Tenant
if (!$JwtSecret) {
    $JwtSecret = [Guid]::NewGuid().ToString("N") + [Guid]::NewGuid().ToString("N")
}
$env:ALTHEA_JWT_SECRET = $JwtSecret
$env:PYTHONUNBUFFERED = '1'

Push-Location $backend
try {
    switch ($Role) {
        'api' {
            Ensure-ApiPortReady -ApiPort $Port
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
