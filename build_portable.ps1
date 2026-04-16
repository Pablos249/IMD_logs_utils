$ErrorActionPreference = "Stop"

$python = ".\.venv\Scripts\python.exe"
$distDir = "dist\IMDLogUtils"
$portableDataDir = Join-Path $distDir "portable_data"
$zipPath = "dist\IMDLogUtils-portable.zip"

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

New-Item -ItemType Directory -Force -Path $portableDataDir | Out-Null
Set-Content -Path (Join-Path $portableDataDir ".gitkeep") -Value "" -Encoding ascii

if (Test-Path $zipPath) {
    Remove-Item -LiteralPath $zipPath -Force
}

Compress-Archive -Path "$distDir\*" -DestinationPath $zipPath

Write-Host ""
Write-Host "Portable build finished."
Write-Host "Folder: $distDir"
Write-Host "Zip:    $zipPath"
