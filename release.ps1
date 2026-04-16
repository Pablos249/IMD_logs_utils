$ErrorActionPreference = "Stop"

param(
    [ValidateSet("patch", "minor", "major")]
    [string]$Bump = "patch",
    [string]$Version
)

$appInfoPath = "src\app_info.py"
$python = ".\.venv\Scripts\python.exe"

if (-not (Test-Path $appInfoPath)) {
    throw "Missing file: $appInfoPath"
}

if (-not (Test-Path $python)) {
    throw "Virtual environment not found at .venv. Create it first and install dependencies."
}

$content = Get-Content -Path $appInfoPath -Raw
$match = [regex]::Match($content, 'APP_VERSION = "(\d+)\.(\d+)\.(\d+)"')
if (-not $match.Success) {
    throw "Could not find APP_VERSION in $appInfoPath"
}

$currentVersion = $match.Groups[1].Value + "." + $match.Groups[2].Value + "." + $match.Groups[3].Value

if ($Version) {
    if ($Version -notmatch '^\d+\.\d+\.\d+$') {
        throw "Version must use semantic versioning format, e.g. 1.2.3"
    }
    $newVersion = $Version
}
else {
    $major = [int]$match.Groups[1].Value
    $minor = [int]$match.Groups[2].Value
    $patch = [int]$match.Groups[3].Value

    switch ($Bump) {
        "major" {
            $major += 1
            $minor = 0
            $patch = 0
        }
        "minor" {
            $minor += 1
            $patch = 0
        }
        default {
            $patch += 1
        }
    }

    $newVersion = "$major.$minor.$patch"
}

$updated = [regex]::Replace($content, 'APP_VERSION = "\d+\.\d+\.\d+"', "APP_VERSION = `"$newVersion`"", 1)
Set-Content -Path $appInfoPath -Value $updated -Encoding utf8

Write-Host "Version bumped: $currentVersion -> $newVersion"

& $python -m py_compile src\app_info.py src\main.py src\ui\main_window.py
& .\build_portable.ps1

$versionedZip = "dist\IMDLogUtils-$newVersion-portable.zip"
Copy-Item -LiteralPath "dist\IMDLogUtils-portable.zip" -Destination $versionedZip -Force

Write-Host ""
Write-Host "Release ready."
Write-Host "Executable folder: dist\IMDLogUtils"
Write-Host "Portable zip:     dist\IMDLogUtils-portable.zip"
Write-Host "Versioned zip:    $versionedZip"
