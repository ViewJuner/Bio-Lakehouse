# One-command đồ án demo
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

Write-Host "Starting Bio Lakehouse (docker compose)..."
docker compose up -d --build

Write-Host ""
Write-Host "Dashboard:  http://localhost:8080"
Write-Host "NiFi:       http://localhost:8081/nifi"
Write-Host "MinIO:      http://localhost:9001  (minioadmin / minioadmin)"
Write-Host "HBase:      http://localhost:16010"
Write-Host ""
Write-Host "Wait ~60-90s for Kafka/HBase/Spark, then refresh the dashboard."
Write-Host "P005 will spike heart rate every ~40s to trigger alerts."

Start-Process "http://localhost:8080"
