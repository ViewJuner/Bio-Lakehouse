$ErrorActionPreference = "Continue"
Write-Host "Checking dashboard health..."
try {
  $h = Invoke-RestMethod "http://localhost:8080/api/health"
  Write-Host ("health: " + ($h | ConvertTo-Json -Compress))
} catch {
  Write-Host "Dashboard not reachable yet. Is docker compose up?"
  exit 1
}

$latest = Invoke-RestMethod "http://localhost:8080/api/latest"
Write-Host ("latest rows: " + $latest.Count)
if ($latest.Count -lt 1) {
  Write-Host "No patient_latest rows yet — wait for spark micro-batch (30-90s) and retry."
  exit 2
}

$alerts = Invoke-RestMethod "http://localhost:8080/api/alerts"
Write-Host ("alerts: " + $alerts.Count)
Write-Host "Pipeline looks alive."
exit 0
