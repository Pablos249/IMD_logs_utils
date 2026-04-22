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
    --add-data "docs;docs" `
    --add-data "CHANGELOG.md;." `
    src\main.py

Write-Host ""
Write-Host "Build finished."
Write-Host "Executable folder: dist\IMDLogUtils"
