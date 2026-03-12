# Set environment variables
$env:ALTHEA_ENV = "development"
$env:ALTHEA_DEFAULT_TENANT_ID = "default-bank"
$env:ALTHEA_DATABASE_URL = "sqlite:///data/althea.db"
$env:ALTHEA_REDIS_URL = "redis://localhost:6379"
$env:ALTHEA_JWT_SECRET = "your-secret-key-min-32-chars-long-enough"
$env:ALTHEA_OBJECT_STORAGE_DIR = "./data/models"

Write-Host "Starting Backend API..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd backend; uvicorn main:app --reload"

Start-Sleep -Seconds 2

Write-Host "Starting Worker..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd backend; python -m workers.pipeline_worker"

Start-Sleep -Seconds 2

Write-Host "Starting Frontend..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd frontend; npm run dev"

Write-Host "All services started!" -ForegroundColor Cyan
Write-Host "Frontend: http://localhost:5173" -ForegroundColor Yellow
Write-Host "API: http://localhost:8000/docs" -ForegroundColor Yellow