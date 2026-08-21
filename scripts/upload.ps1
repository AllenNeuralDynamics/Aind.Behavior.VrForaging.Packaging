# ============================================================
# Process, aggregate, and (optionally) upload to s3://aind-scratch-data/vr-foraging/
#
# 1. Runs vr-foraging-packaging batch (clean + full pipeline) against the integration cache.
# 2. Uploads the output to an S3 prefix (skipped with -SkipUpload).
#
# Usage:
#   .\upload.ps1               # process + upload
#   .\upload.ps1 -SkipUpload   # process only (no S3)
# ============================================================

param(
    [switch]$SkipUpload
)

# --- EDIT THIS VALUE ---
$ProfileName  = "aind-scientist"               # your AWS SSO profile name
# -----------------------

$RepoRoot     = "$PSScriptRoot\.."
$InputDir     = "$RepoRoot\tests\integration\.cache\aind-open-data"   # S3 download cache (sessions are bucket-namespaced)
$OutputDir    = "$RepoRoot\scratch\export"             # pipeline writes here (cleaned on each run)
$SubFolder    = "demo"
$Destination  = "s3://aind-scratch-data/vr-foraging/$SubFolder"

# --- Pre-flight checks ---

if (-not (Test-Path $InputDir)) {
    Write-Host "Integration cache not found: $InputDir" -ForegroundColor Red
    Write-Host "Run the integration test suite first to populate the cache." -ForegroundColor Yellow
    exit 1
}

if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
    Write-Host "AWS CLI not found. Install it from https://aws.amazon.com/cli/ and try again." -ForegroundColor Red
    exit 1
}

# --- Phase 1 + 2: process and aggregate ---

Write-Host "Running export pipeline: $InputDir -> $OutputDir ..."
uv run vr-foraging-packaging batch --input-dir $InputDir --output-dir $OutputDir --write-nwb
if ($LASTEXITCODE -ne 0) {
    Write-Host "Pipeline failed. Aborting upload." -ForegroundColor Red
    exit 1
}

if ($SkipUpload) {
    Write-Host "Skipping upload (-SkipUpload set). Output is at: $OutputDir" -ForegroundColor Cyan
    exit 0
}

# --- Authenticate ---

Write-Host "Checking AWS SSO session for profile '$ProfileName'..."
aws sts get-caller-identity --profile $ProfileName *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Session expired or not logged in. Opening browser for SSO login..."
    aws sso login --profile $ProfileName
    if ($LASTEXITCODE -ne 0) {
        Write-Host "SSO login failed." -ForegroundColor Red
        exit 1
    }
}

# --- Upload ---

Write-Host "Uploading '$OutputDir' to '$Destination' ..."
aws s3 sync $OutputDir $Destination --profile $ProfileName

if ($LASTEXITCODE -eq 0) {
    Write-Host "Upload complete: $Destination" -ForegroundColor Green
} else {
    Write-Host "Upload finished with errors. Check the output above." -ForegroundColor Yellow
}
