$ErrorActionPreference = "Stop"

$python = ".\.venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    throw "Virtual environment not found at .venv. Create it first and install dependencies."
}

& $python -m PyInstaller `
    --noconfirm `
    --clean `
    --windowed `
    --name IMDLogUtils `
    --paths . `
    src\main.py

Write-Host ""
Write-Host "Build finished."
Write-Host "Executable folder: dist\IMDLogUtils"
