# LinkPlease Instagram DM Automation Engine

LinkPlease automates Instagram DMs for creators. When a follower comments a keyword like `PRICE` on a creator's post, LinkPlease automatically sends them a direct message with the price list or product details.

This backend service is designed to process high-throughput comment webhooks, handle rate limits, eliminate duplicate messages, recover from process crashes, and reconcile asynchronous delivery statuses against a hostile platform API.

---

## 📐 End-to-End System Architecture

```
Webhook (POST /webhook)
   │
   ▼
Event Persistence & Signature Check (HMAC-SHA256)
   │
   ▼
Event Deduplication (PRIMARY KEY on events.event_id)
   │
   ▼
Rule Keyword Matching (Case-insensitive substring matching)
   │
   ▼
Delivery Deduplication (PRIMARY KEY (user_id, rule_id) on user_rule_matches)
   │
   ▼
Persistent SQLite Delivery Queue (dm_tasks table)
   │
   ▼
Rate-Limited Outbound Worker (Max 10 req / rolling 60s)
   │
   ▼
PseudoGram DM API (POST /v1/dm/send with Idempotency-Key)
   │
   ├── 202 Accepted ──> Save dm_id, api_status = 'queued'
   ├── 429 Limit    ──> Respect Retry-After header, reschedule
   ├── 500 Error    ──> Exponential backoff (1s, 2s, 4s, 8s...)
   └── 400 Error    ──> Mark status = 'failed' (permanent)
   │
   ▼
Reconciliation Worker (GET /v1/dm/{dm_id})
   ├── 'delivered'  ──> api_status = 'delivered' (sent += 1)
   └── 'failed'     ──> Re-queue if retries remaining
   │
   ▼
Live Aggregated Stats (GET /stats)
```

---

## 🛠️ Tech Stack

- **Core**: Python 3.14 / Python 3.11+
- **Web Framework**: FastAPI + Uvicorn (Asynchronous, high-throughput ASGI engine)
- **Database & Persistence**: SQLite in **WAL (Write-Ahead Logging)** mode (`PRAGMA journal_mode=WAL;`). Ensures durable, crash-resilient queueing without external service dependencies.
- **HTTP Client**: `httpx.AsyncClient` with connection pooling and timeouts.
- **Containerization**: Docker & Docker Compose.

---

## 🔑 Environment Variables

| Variable | Default Value | Description |
|---|---|---|
| `PSEUDOGRAM_BASE_URL` | `https://pseudogram-api.onrender.com` | Base URL of the mock platform API |
| `PSEUDOGRAM_API_KEY` | *(Required)* | API key sent as `X-API-Key` on requests |
| `DB_PATH` | `linkplease.db` | Filepath to SQLite database |
| `VERIFY_WEBHOOK_SIGNATURE` | `true` | Enables HMAC-SHA256 `X-PseudoGram-Signature` verification |
| `RATE_LIMIT_MAX_REQUESTS` | `10` | Max outbound send requests in rolling window |
| `RATE_LIMIT_WINDOW_SECONDS` | `60.0` | Rolling rate limit window in seconds |
| `MAX_RETRIES` | `5` | Maximum retry attempts for 500/failed deliveries |

---

## 🚀 Local Setup & Installation

### 1. Repository Setup
```bash
git clone https://github.com/your-username/linkplease.git
cd linkplease

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment
Copy `.env.example` to `.env` and set your API key:
```bash
cp .env.example .env
```

### 3. Run FastAPI Application Server
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```
Interactive OpenAPI documentation will be accessible at:
- **Swagger UI**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **ReDoc**: [http://localhost:8000/redoc](http://localhost:8000/redoc)

---

## 📡 API Contract

### `POST /rules`
Creates an automated DM trigger rule.
- **Request**:
  ```json
  { "keyword": "PRICE", "dm_message": "Here's the price catalog: https://example.com" }
  ```
- **Response `201 Created`**:
  ```json
  { "rule_id": "rule_8e4b1529e1", "keyword": "PRICE", "dm_message": "..." }
  ```

### `POST /webhook`
Receives comment webhooks. Returns `200 OK` in **< 5ms**.
- **Request**:
  ```json
  {
    "event_id": "evt_01J8ZQ4K2N7RXA",
    "event_type": "comment.created",
    "sent_at": "2026-08-10T09:14:22.481Z",
    "data": {
      "comment_id": "cmt_9f2a7c",
      "text": "PRICE please 🙏",
      "from": { "user_id": "usr_3b91fe", "username": "arjun.shoots" }
    }
  }
  ```
- **Headers**: `X-PseudoGram-Signature: sha256=<hex>` (HMAC-SHA256 of raw body using API key).

### `GET /stats`
Returns live aggregates calculated directly via SQLite queries:
```json
{
  "sent": 142,
  "failed": 3,
  "queued": 8,
  "duplicates_blocked": 57
}
```

---

## 🛡️ Reliability & Resilience Features

- **Event Idempotency**: Atomic `INSERT OR IGNORE INTO events` with `PRIMARY KEY(event_id)` discards repeated `event_id` occurrences.
- **Delivery Idempotency**: `PRIMARY KEY (user_id, rule_id)` on `user_rule_matches` ensures a user receives at most one DM per rule regardless of how many times they comment. Duplicate attempts increment `duplicates_blocked`.
- **Request Idempotency Key**: Every outbound request passes `Idempotency-Key: rule_{rule_id}_user_{user_id}`. If an HTTP response is lost over the network, retrying with the same idempotency key returns the original `dm_id` without sending a second DM.
- **Rate Limiting**: Worker paces requests at max 10 requests / 60 seconds. High volume spikes (e.g. 500 comments in 10s) are safely queued in SQLite.
- **`comment.deleted` Handling**: If `comment.deleted` arrives before a DM is sent, the task is marked as `failed` and cancelled. If the DM was already delivered, its status remains `delivered`.
- **Crash Recovery**: If the process crashes while a job is in `'processing'` state, `recover_stuck_processing_tasks()` resets stale jobs back to `'queued'` after a 30-second timeout.
- **Reconciliation Engine**: Polls `GET /v1/dm/{dm_id}` to detect async delivery failures (~15%) and automatically re-queues them with exponential backoff.

---

## 🧪 Testing

### Run Automated Unit Test Suite
```bash
pytest test_system.py -v
```

### Run 500-Event Simulator Test
Expose local server via public tunnel (e.g. `npx localtunnel` or `ssh -R 80:localhost:8000 nokey@localhost.run`) and execute:
```bash
python test_simulation.py <public_tunnel_url> 500 10
```
