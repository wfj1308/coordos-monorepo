param(
    [string]$BaseUrl = "http://127.0.0.1:8090",
    [int]$Year = (Get-Date).Year,
    [string]$OutFile = "",
    [string]$TmpJson = "",
    [switch]$KeepJson
)

$ErrorActionPreference = "Stop"

# Resolve output filename if not provided.
if ([string]::IsNullOrWhiteSpace($OutFile)) {
    $OutFile = "qualification_report_$Year.docx"
}

# Resolve temp JSON path if not provided.
if ([string]::IsNullOrWhiteSpace($TmpJson)) {
    $stamp = Get-Date -Format "yyyyMMddHHmmss"
    $TmpJson = Join-Path $env:TEMP "coordos_qualification_$Year_$stamp.json"
}

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$generator = Join-Path $root "scripts\gen_qual_report.js"
$goGenerator = "./services/design-institute/cmd/qual-report"

$url = "$BaseUrl/api/v1/reports/qualification?year=$Year"
Write-Host "[INFO] Fetching report JSON: $url" -ForegroundColor Cyan

try {
    $resp = Invoke-WebRequest -UseBasicParsing -Method GET -Uri $url -TimeoutSec 30 -ErrorAction Stop
} catch {
    throw "failed to fetch report JSON from $url`n$($_.Exception.Message)"
}

# Validate that payload is JSON.
try {
    $obj = $resp.Content | ConvertFrom-Json
} catch {
    throw "response is not valid JSON"
}

# Save normalized JSON for traceability.
$obj | ConvertTo-Json -Depth 20 | Set-Content -Path $TmpJson -Encoding UTF8
Write-Host "[INFO] JSON saved: $TmpJson" -ForegroundColor Cyan

Write-Host "[INFO] Generating DOCX: $OutFile" -ForegroundColor Cyan
$generated = $false

# Prefer rich Node.js generator when dependency is available.
if (Test-Path $generator) {
    $nodeCmd = Get-Command node -ErrorAction SilentlyContinue
    if ($nodeCmd) {
        $nodeDepReady = $false
        try {
            & node -e "require('docx')" *> $null
            if ($LASTEXITCODE -eq 0) {
                $nodeDepReady = $true
            }
        } catch {
            $nodeDepReady = $false
        }

        if ($nodeDepReady) {
            & node $generator $TmpJson $OutFile
            if ($LASTEXITCODE -eq 0) {
                $generated = $true
            } else {
                Write-Host "[WARN] gen_qual_report.js failed (exit $LASTEXITCODE), fallback to Go generator..." -ForegroundColor Yellow
            }
        } else {
            Write-Host "[WARN] docx dependency missing in Node.js environment, fallback to Go generator..." -ForegroundColor Yellow
        }
    } else {
        Write-Host "[WARN] node not found, fallback to Go generator..." -ForegroundColor Yellow
    }
} else {
    Write-Host "[WARN] JS generator missing: $generator, fallback to Go generator..." -ForegroundColor Yellow
}

# Fallback path: stdlib-only Go generator for offline environments.
if (-not $generated) {
    Push-Location $root
    try {
        $repoGoCache = Join-Path $root ".gocache"
        $repoGoModCache = Join-Path $root ".gomodcache"
        $repoGoTelemetryDir = Join-Path $root ".gotelemetry"
        $repoAppData = Join-Path $root ".appdata"
        New-Item -ItemType Directory -Path $repoGoCache -Force | Out-Null
        New-Item -ItemType Directory -Path $repoGoModCache -Force | Out-Null
        New-Item -ItemType Directory -Path $repoGoTelemetryDir -Force | Out-Null
        New-Item -ItemType Directory -Path $repoAppData -Force | Out-Null

        $oldGoCache = $env:GOCACHE
        $oldGoModCache = $env:GOMODCACHE
        $oldGoTelemetry = $env:GOTELEMETRY
        $oldGoTelemetryDir = $env:GOTELEMETRYDIR
        $oldAppData = $env:APPDATA
        $env:GOCACHE = $repoGoCache
        $env:GOMODCACHE = $repoGoModCache
        $env:GOTELEMETRY = "local"
        $env:GOTELEMETRYDIR = $repoGoTelemetryDir
        $env:APPDATA = $repoAppData

        try {
            & go run $goGenerator --in $TmpJson --out $OutFile
            if ($LASTEXITCODE -ne 0) {
                throw "go generator failed with exit code $LASTEXITCODE"
            }
            $generated = $true
        } finally {
            $env:GOCACHE = $oldGoCache
            $env:GOMODCACHE = $oldGoModCache
            $env:GOTELEMETRY = $oldGoTelemetry
            $env:GOTELEMETRYDIR = $oldGoTelemetryDir
            $env:APPDATA = $oldAppData
        }
    } finally {
        Pop-Location
    }
}

if (-not $generated) {
    throw "failed to generate report via both JS and Go generators"
}

if (-not $KeepJson) {
    Remove-Item -Path $TmpJson -ErrorAction SilentlyContinue
} else {
    Write-Host "[INFO] Keeping JSON: $TmpJson" -ForegroundColor Cyan
}

Write-Host "[PASS] Generated report: $OutFile" -ForegroundColor Green
