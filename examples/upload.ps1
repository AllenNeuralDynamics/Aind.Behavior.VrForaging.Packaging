# ============================================================
# Upload a local folder to s3://aind-scratch-data/vr-foraging/
# ============================================================

# --- EDIT THIS VALUE ---
$ProfileName  = "aind-scientist"               # your AWS SSO profile name
# -----------------------

$LocalFolder  = "$PSScriptRoot\..\scratch\export"  # contents uploaded into S3 subfolder
$SubFolder    = "test"                             # destination subfolder inside vr-foraging/
$Destination  = "s3://aind-scratch-data/vr-foraging/$SubFolder"

# Check AWS CLI is installed
if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
    Write-Host "AWS CLI not found. Install it from https://aws.amazon.com/cli/ and try again." -ForegroundColor Red
    exit 1
}

# Check local folder exists
if (-not (Test-Path $LocalFolder)) {
    Write-Host "Local folder not found: $LocalFolder" -ForegroundColor Red
    exit 1
}

# Check / refresh SSO session
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

# Sync the folder
Write-Host "Uploading '$LocalFolder' to '$Destination' ..."
aws s3 sync $LocalFolder $Destination --profile $ProfileName

if ($LASTEXITCODE -eq 0) {
    Write-Host "Upload complete." -ForegroundColor Green
} else {
    Write-Host "Upload finished with errors. Check the output above." -ForegroundColor Yellow
}