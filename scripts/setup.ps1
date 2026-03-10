# setup.ps1 - Windows PowerShell Setup Script
# Fuehrt einmalig aus: venv erstellen + alle Abhaengigkeiten installieren
# Aufruf: .\scripts\setup.ps1  (aus dem Repository-Root)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot

Write-Host "=== Masterthesis Analytics - Environment Setup ===" -ForegroundColor Cyan
Write-Host "Repository: $RepoRoot"

# 1. Virtuelle Umgebung anlegen (falls noch nicht vorhanden)
$VenvPath = Join-Path $RepoRoot ".venv"
if (-Not (Test-Path $VenvPath)) {
    Write-Host "`n[1/3] Erstelle virtuelle Umgebung..." -ForegroundColor Yellow
    python -m venv $VenvPath
} else {
    Write-Host "`n[1/3] Virtuelle Umgebung bereits vorhanden, wird uebersprungen." -ForegroundColor Green
}

# 2. Pip upgraden + Pakete installieren
Write-Host "`n[2/3] Installiere Abhaengigkeiten aus requirements.txt..." -ForegroundColor Yellow
$PipExe = Join-Path $VenvPath "Scripts\pip.exe"
& $PipExe install --upgrade pip --quiet
& $PipExe install -r (Join-Path $RepoRoot "requirements.txt")

# 3. Sanity-Check: Imports testen
Write-Host "`n[3/3] Fuehre Import-Sanity-Check aus..." -ForegroundColor Yellow
$PythonExe = Join-Path $VenvPath "Scripts\python.exe"
& $PythonExe -m pytest (Join-Path $RepoRoot "tests\test_sanity.py") -v --no-header

Write-Host "`n=== Setup abgeschlossen ===" -ForegroundColor Green
Write-Host "Umgebung aktivieren: .\.venv\Scripts\Activate.ps1"
