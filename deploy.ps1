# CineCloud — GCP Deployment Script (PowerShell)
# Run this ONCE to provision all GCP resources and deploy all Cloud Functions
#
# Prerequisites:
#   1. Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install
#   2. Run: gcloud auth login
#   3. Create a GCP project at console.cloud.google.com
#   4. Add Firebase to the project at console.firebase.google.com
#   5. Fill in the variables below (or set them as env vars)
#
# Usage (from project root):
#   .\deploy.ps1
#
# Estimated time: ~5-8 minutes for first deploy, ~2-3 min for updates

param(
    [string]$ProjectId    = $env:GCP_PROJECT_ID,
    [string]$Region       = "us-central1",
    [string]$TmdbApiKey   = $env:TMDB_API_KEY,
    [string]$RedisUrl     = $env:REDIS_URL,
    [string]$FirebaseKey  = $env:FIREBASE_WEB_API_KEY
)

# ── Validate required parameters ────────────────────────────
if (-not $ProjectId) {
    Write-Error "GCP_PROJECT_ID is required. Set it as an env var or pass -ProjectId"
    exit 1
}
if (-not $TmdbApiKey) {
    Write-Error "TMDB_API_KEY is required. Set it as an env var or pass -TmdbApiKey"
    exit 1
}

Write-Host ""
Write-Host "===================================================" -ForegroundColor Cyan
Write-Host "  CineCloud -- GCP Deployment" -ForegroundColor Cyan
Write-Host "  Project: $ProjectId" -ForegroundColor Cyan
Write-Host "===================================================" -ForegroundColor Cyan
Write-Host ""

# ── Step 1: Configure project ────────────────────────────────
Write-Host "[1/7] Configuring GCP project..." -ForegroundColor Yellow
gcloud config set project $ProjectId

# ── Step 2: Enable required APIs ────────────────────────────
Write-Host "[2/7] Enabling GCP APIs (this may take a minute)..." -ForegroundColor Yellow
$apis = @(
    "cloudfunctions.googleapis.com",
    "run.googleapis.com",              # Cloud Functions Gen 2 uses Cloud Run
    "cloudbuild.googleapis.com",       # Required for function builds
    "firestore.googleapis.com",
    "pubsub.googleapis.com",
    "storage.googleapis.com",
    "cloudscheduler.googleapis.com",   # For ETL cron
    "monitoring.googleapis.com",
    "logging.googleapis.com"
)
foreach ($api in $apis) {
    gcloud services enable $api --quiet
    Write-Host "  ✓ $api" -ForegroundColor Green
}

# ── Step 3: Create Firestore database ────────────────────────
Write-Host "[3/7] Creating Firestore database..." -ForegroundColor Yellow
$firestoreCheck = gcloud firestore databases list '--format=value(name)' 2>$null
if (-not $firestoreCheck) {
    gcloud firestore databases create --location=$Region --quiet
    Write-Host "  ✓ Firestore database created in $Region" -ForegroundColor Green
} else {
    Write-Host "  ✓ Firestore already exists, skipping" -ForegroundColor Green
}

# ── Step 4: Create Pub/Sub topics ───────────────────────────
Write-Host "[4/7] Creating Pub/Sub topics..." -ForegroundColor Yellow
$topicCheck = gcloud pubsub topics list '--filter=rating-events' '--format=value(name)' 2>$null
if (-not $topicCheck) {
    gcloud pubsub topics create rating-events
    gcloud pubsub topics create rating-events-dlq
    Write-Host "  ✓ Pub/Sub topics created: rating-events, rating-events-dlq" -ForegroundColor Green
} else {
    Write-Host "  ✓ Pub/Sub topics already exist, skipping" -ForegroundColor Green
}

# ── Step 5: Create Cloud Storage bucket ─────────────────────
Write-Host "[5/7] Creating Cloud Storage bucket..." -ForegroundColor Yellow
$bucketName = "$ProjectId-cinecloud-assets"
$bucketCheck = gsutil ls "gs://$bucketName" 2>$null
if (-not $bucketCheck) {
    gsutil mb -l $Region "gs://$bucketName"
    gsutil iam ch allUsers:objectViewer "gs://$bucketName"
    Write-Host "  ✓ Storage bucket created: gs://$bucketName" -ForegroundColor Green
} else {
    Write-Host "  ✓ Storage bucket already exists, skipping" -ForegroundColor Green
}

# ── Step 6: Copy shared requirements.txt to each function ───
Write-Host "[6/7] Preparing function source directories..." -ForegroundColor Yellow
$functionDirs = @(
    "functions\auth_service",
    "functions\movie_service",
    "functions\etl_pipeline",
    "functions\rating_service",
    "functions\recommendation_service",
    "functions\event_tracker"
)
foreach ($dir in $functionDirs) {
    Copy-Item -Force requirements.txt "$dir\requirements.txt"
    Write-Host "  ✓ requirements.txt copied to $dir" -ForegroundColor Green
}

# ── Step 7: Deploy Cloud Functions ──────────────────────────
Write-Host "[7/7] Deploying Cloud Functions to GCP..." -ForegroundColor Yellow
Write-Host "  (Each function takes ~1-2 minutes on first deploy)" -ForegroundColor Gray

# Common env vars for all functions
$commonEnvVars = "GCP_PROJECT_ID=$ProjectId,TMDB_API_KEY=$TmdbApiKey,FIREBASE_WEB_API_KEY=$FirebaseKey,REDIS_URL=$RedisUrl"

$functions = @(
    @{
        Name       = "cinecloud-auth"
        Dir        = "functions\auth_service"
        EntryPoint = "http_handler"
        Trigger    = "--trigger-http --allow-unauthenticated"
        Memory     = "256Mi"
    },
    @{
        Name       = "cinecloud-movies"
        Dir        = "functions\movie_service"
        EntryPoint = "http_handler"
        Trigger    = "--trigger-http --allow-unauthenticated"
        Memory     = "256Mi"
    },
    @{
        Name       = "cinecloud-ingest"
        Dir        = "functions\etl_pipeline"
        EntryPoint = "http_handler"
        Trigger    = "--trigger-http --no-allow-unauthenticated"
        Memory     = "512Mi"
    },
    @{
        Name       = "cinecloud-ratings"
        Dir        = "functions\rating_service"
        EntryPoint = "http_handler"
        Trigger    = "--trigger-http --allow-unauthenticated"
        Memory     = "256Mi"
    },
    @{
        Name       = "cinecloud-recommendations"
        Dir        = "functions\recommendation_service"
        EntryPoint = "http_handler"
        Trigger    = "--trigger-http --allow-unauthenticated"
        Memory     = "512Mi"
    },
    @{
        Name       = "cinecloud-rec-worker"
        Dir        = "functions\recommendation_service"
        EntryPoint = "pubsub_handler"
        Trigger    = "--trigger-topic=rating-events"
        Memory     = "512Mi"
    },
    @{
        Name       = "cinecloud-events"
        Dir        = "functions\event_tracker"
        EntryPoint = "http_handler"
        Trigger    = "--trigger-http --allow-unauthenticated"
        Memory     = "128Mi"
    }
)

foreach ($fn in $functions) {
    Write-Host "  Deploying $($fn.Name)..." -ForegroundColor Cyan
    $cmd = "gcloud functions deploy $($fn.Name) " +
           "--gen2 " +
           "--runtime=python311 " +
           "--region=$Region " +
           "--source=$($fn.Dir) " +
           "--entry-point=$($fn.EntryPoint) " +
           "--memory=$($fn.Memory) " +
           "--timeout=300s " +
           "--set-env-vars=`"$commonEnvVars`" " +
           "$($fn.Trigger) " +
           "--quiet"
    
    Invoke-Expression $cmd
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  ✓ $($fn.Name) deployed successfully" -ForegroundColor Green
    } else {
        Write-Host "  ✗ $($fn.Name) deploy FAILED (see error above)" -ForegroundColor Red
    }
}

# ── Print function URLs ──────────────────────────────────────
Write-Host ""
Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  Deployment complete! Function URLs:" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Cyan

$httpFunctions = @("cinecloud-auth", "cinecloud-movies", "cinecloud-ratings", "cinecloud-recommendations", "cinecloud-events")
foreach ($fn in $httpFunctions) {
    $url = gcloud functions describe $fn --gen2 --region=$Region --format="value(serviceConfig.uri)" 2>$null
    Write-Host "  $fn" -ForegroundColor White
    Write-Host "    $url" -ForegroundColor Gray
}

Write-Host ""
Write-Host "  Next steps:" -ForegroundColor Yellow
Write-Host "  1. Copy the URLs above into frontend/.env.local" -ForegroundColor White
Write-Host "  2. Run the movie ingest (POST /admin/ingest to cinecloud-ingest)" -ForegroundColor White
Write-Host "  3. cd frontend && npm run build && vercel deploy" -ForegroundColor White
Write-Host ""
Write-Host "  See README.md for full setup instructions." -ForegroundColor Gray
