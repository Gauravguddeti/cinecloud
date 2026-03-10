# CineCloud 🎬 — Cloud-Native Movie Recommendation Engine

> **College Cloud Computing Project** — A production-grade, fully serverless movie recommendation system built on **Google Cloud Platform**, demonstrating 12+ cloud computing concepts including real-time data sync, microservices, event-driven architecture, and a custom ML hybrid recommendation engine.

## Architecture Overview

```
 User Browser (Vercel Edge CDN)
       │  HTTPS            │  Firestore onSnapshot (real-time ⚡)
       ▼                   ▼
 GCP Cloud Functions (HTTP triggers, Gen 2)
  ┌──────────────────────────────────────────────┐
  │  cinecloud-auth          (Firebase Auth)      │
  │  cinecloud-movies        (Firestore catalog)  │
  │  cinecloud-ratings       (Firestore + Pub/Sub)│
  │  cinecloud-recommendations (CF+CBF algorithm) │
  │  cinecloud-ingest        (TMDB → Firestore)   │
  │  cinecloud-events        (analytics logging)  │
  └──────────────────────────────────────────────┘
       │                                  │
       ▼                                  ▼
 Firestore (NoSQL)               Cloud Pub/Sub
 ┌─────────────┐                (rating-events topic)
 │ users       │                         │
 │ movies      │                         ▼
 │ ratings     │              cinecloud-rec-worker
 │ recommendations ◄─── writes updated recs (Pub/Sub trigger)
 │ events      │              (Firestore onSnapshot fires on frontend⚡)
 └─────────────┘
       │
       ▼
 Upstash Redis (two-tier cache)   Firebase Auth (JWT)
 Cloud Storage (assets)           Cloud Monitoring
```

## Cloud Computing Concepts Demonstrated

| # | Concept | GCP Implementation |
|---|---------|-------------------|
| 1 | **Serverless Computing** | Cloud Functions Gen 2 — auto-scales to zero |
| 2 | **Microservices Architecture** | 7 independent Cloud Functions |
| 3 | **Event-Driven Architecture** | Rating → Pub/Sub → worker Cloud Function |
| 4 | **Message Queuing** | Cloud Pub/Sub + dead-letter topic |
| 5 | **NoSQL Cloud Database** | Firestore (collections: users, movies, ratings, recommendations) |
| 6 | **Distributed Caching** | Upstash Redis (two-tier: Redis 30min + Firestore 24h) |
| 7 | **CDN** | Cloud CDN + Vercel Edge CDN (frontend) |
| 8 | **API Gateway** | Cloud Functions HTTP triggers with CORS + JWT auth |
| 9 | **Cloud Storage** | GCS bucket for movie assets |
| 10 | **Infrastructure as Code** | `deploy.ps1` (gcloud CLI automation) |
| 11 | **Real-Time Data Sync** | Firestore `onSnapshot` — live rec updates, no WebSocket infra needed ⚡ |
| 12 | **Cloud Auth & IAM** | Firebase Authentication + Firebase Admin SDK |
| 13 | **Monitoring & Observability** | Cloud Monitoring + Cloud Logging (structured logs) |

## Recommendation Algorithm

```
score(movie) = 0.70 × CF_score + 0.30 × CBF_score

CF  = User-User Collaborative Filtering (cosine similarity on ratings matrix)
CBF = Content-Based Filtering (TF-IDF on genres + cast + keywords)

Cold start → falls back to popularity-ranked movies
```

## Real-Time Updates (Firestore onSnapshot)

Instead of managing WebSocket infrastructure, CineCloud uses Firestore's built-in real-time sync:
1. User rates a movie → Cloud Function writes to Firestore `ratings/` + publishes to Pub/Sub
2. Pub/Sub triggers `cinecloud-rec-worker`
3. Worker recomputes recommendations → writes to Firestore `recommendations/{userId}`
4. Frontend `onSnapshot` listener fires instantly — UI updates with new personalized recommendations ⚡

This is more reliable, offline-capable, and requires zero extra infrastructure vs WebSockets.

## Project Structure

```
project1/
├── deploy.ps1                  ← One-command GCP deployment script (PowerShell)
├── requirements.txt            ← Python dependencies (shared, copied to each function)
├── .env.example                ← backend env vars template
├── .env                        ← ACTUAL secrets (gitignored, TMDB key pre-filled)
├── functions/
│   ├── auth_service/
│   │   ├── main.py             ← GCP: Firebase Auth register/login/profile
│   │   └── handler.py          ← (AWS version, kept for reference)
│   ├── movie_service/
│   │   ├── main.py             ← GCP: Firestore movie catalog API
│   │   └── handler.py          ← (AWS version, kept for reference)
│   ├── etl_pipeline/
│   │   ├── main.py             ← GCP: TMDB → Firestore batch ingestion
│   │   └── handler.py          ← (AWS version)
│   ├── rating_service/
│   │   ├── main.py             ← GCP: Firestore ratings + Pub/Sub publish
│   │   └── handler.py          ← (AWS version)
│   ├── recommendation_service/
│   │   ├── main.py             ← GCP: Hybrid CF+CBF algo + Pub/Sub worker
│   │   ├── handler.py          ← (AWS version)
│   │   └── worker.py           ← (AWS version — SQS consumer)
│   └── event_tracker/
│       ├── main.py             ← GCP: HTTP event logging
│       └── handler.py          ← (AWS version — WebSocket)
└── frontend/                   ← Next.js 14 app (deploy to Vercel)
    ├── src/
    │   ├── app/                ← Pages: home, browse, login, register, profile
    │   ├── components/         ← Navbar, MovieCard, StarRating, Modal, AppProvider
    │   ├── hooks/              ← useRealtimeRecs.ts (Firestore onSnapshot)
    │   └── lib/                ← api.ts, store.ts, types.ts, firebase.ts
    └── .env.example            ← frontend env vars template (Firebase + GCP URLs)
```

## Setup & Deployment

### Prerequisites
- Node.js 18+, Python 3.11+
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) (`gcloud` CLI)
- GCP account (free $300 credit for new accounts)
- TMDB API key v3 — already in `.env` ✅
- Upstash Redis connection URL (free at https://upstash.com)

### 1. Create GCP Project & Enable Firebase
1. Go to [console.cloud.google.com](https://console.cloud.google.com) → Create Project
2. Note your **Project ID** (e.g., `cinecloud-yourname-123`)
3. Go to [console.firebase.google.com](https://console.firebase.google.com) → Add Firebase to your project
4. In Firebase Console → Authentication → Sign-in method → Enable **Email/Password**
5. In Firebase Console → Project Settings → copy **Web API Key**

### 2. Configure environment variables
```bash
# Fill in your values in .env:
#   GCP_PROJECT_ID=your-project-id
#   FIREBASE_WEB_API_KEY=AIzaSy_your_key
#   REDIS_URL=redis://default:PASSWORD@endpoint.upstash.io:6379
```

> **Upstash Redis**: The key `5471b7da-...` is your account API key, not the Redis URL.
> In Upstash console → Create Redis database → copy the **Redis URL** (starts with `redis://`)

### 3. Authenticate gcloud CLI
```powershell
gcloud auth login
gcloud auth application-default login
```

### 4. Deploy everything to GCP
```powershell
# From project root:
$env:GCP_PROJECT_ID="your-project-id"
$env:TMDB_API_KEY="52f80a34c6680853389c9df805625bca"
$env:FIREBASE_WEB_API_KEY="your-firebase-web-api-key"
$env:REDIS_URL="redis://default:PASSWORD@endpoint.upstash.io:6379"

.\deploy.ps1
```

This will:
- Enable all required GCP APIs
- Create Firestore database
- Create Pub/Sub topics (`rating-events`, `rating-events-dlq`)
- Create Cloud Storage bucket
- Deploy all 7 Cloud Functions
- Print each function's HTTPS URL

### 5. Ingest movie data from TMDB
```bash
# Call the ingest Cloud Function (replace URL with cinecloud-ingest URL from deploy output)
curl -X POST https://cinecloud-ingest-XXXX-uc.a.run.app/admin/ingest \
     -H "Content-Type: application/json" \
     -d '{"pages": 5}'
# This fetches ~100 popular movies from TMDB into Firestore
```

### 6. Deploy frontend to Vercel
```powershell
cd frontend
# Copy .env.example to .env.local and fill in:
#   - All 5 NEXT_PUBLIC_*_API_URL values from deploy.ps1 output
#   - All 6 NEXT_PUBLIC_FIREBASE_* values from Firebase Console
npm install
npm run build
npx vercel deploy
```
```bash
npm run deploy:dev
# After deploy, copy the outputs (API URL, WS URL, Cognito IDs)
```

### 5. Seed the movie catalog
```bash
# Invoke the ETL pipeline (fetches 10 pages × 20 movies = ~200 movies)
serverless invoke -f ingestMovies --data '{"pages": 10}'
```

### 6. Deploy frontend
```bash
cd frontend
cp .env.example .env.local
# Fill in the URLs from step 4
npm install
npm run build
# Deploy to Vercel:
npx vercel --prod
```

## API Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| POST | `/auth/register` | — | Register new user |
| POST | `/auth/login` | — | Login, get JWT tokens |
| POST | `/auth/refresh` | — | Refresh access token |
| GET | `/auth/profile` | ✓ JWT | Get user profile |
| GET | `/movies` | — | List movies (paginated) |
| GET | `/movies/search?q=` | — | Search movies |
| GET | `/movies/popular` | — | Top movies by popularity |
| GET | `/movies/genres` | — | All genres |
| GET | `/movies/{id}` | — | Movie detail |
| POST | `/admin/ingest` | — | Trigger ETL pipeline |
| POST | `/ratings` | ✓ JWT | Submit/update rating |
| GET | `/ratings/{userId}` | ✓ JWT | Get user's ratings |
| GET | `/recommendations/{userId}` | ✓ JWT | Get recommendations |
| POST | `/recommendations/{userId}/refresh` | ✓ JWT | Force recompute |

## Real-Time Flow (Demo for Professor)

1. User rates a movie → POST `/ratings` → DynamoDB write + SQS publish
2. SQS triggers `recommendationWorker` Lambda (async)
3. Worker recomputes hybrid CF+CBF recommendations
4. New recommendations cached in Redis (30 min TTL)
5. Worker finds user's WebSocket connection IDs in DynamoDB
6. Worker pushes updated recommendations via API Gateway WebSocket
7. Frontend receives `RECOMMENDATIONS_UPDATED` message
8. UI updates with a toast notification — no page refresh needed ⚡

## Cost (AWS Free Tier)

| Service | Free Tier | Actual Usage |
|---------|-----------|-------------|
| Lambda | 1M req/month | ~10K req demo |
| DynamoDB | 25 GB + 25 WCU/RCU | < 1 GB |
| SQS | 1M messages/month | ~1K messages |
| S3 | 5 GB | < 100 MB |
| Cognito | 50,000 MAU | demo users |
| CloudFront | 1 TB transfer | minimal |
| **Total** | | **$0** |
