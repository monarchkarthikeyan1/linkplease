# System Failure Modes — LinkPlease Instagram DM Automation

Below is an honest, technical breakdown of edge cases, system limitations, and conditions under which our system could drop a DM, experience delay, or report discrepancies.

---

### 1. Database Failure or Disk I/O Block During Webhook Arrival
- **Condition**: If the underlying SQLite database file is locked, corrupted, or out of disk space when a webhook arrives at `POST /webhook`.
- **Impact**: The database write for `INSERT INTO events` or `INSERT INTO user_rule_matches` will fail. FastAPI will return HTTP `500` to the platform webhook sender.
- **Result**: The event is not persisted locally. The mock platform will attempt webhook redelivery later; if the database remains down, the event is lost.

---

### 2. Process Crash During In-Flight Outbound API Call (Pre-Response Loss Window)
- **Condition**: The outbound DM worker acquires a task, sets state to `'processing'`, issues `POST /v1/dm/send` to PseudoGram, and the process experiences a hard crash (`SIGKILL` or power failure) before receiving the HTTP response or writing `dm_id` to disk.
- **Impact**: On restart, our crash recovery module (`recover_stuck_processing_tasks`) resets the task back to `'queued'` after the 30-second timeout.
- **Mitigation**: When the worker retries the send request, it passes the exact same deterministic header:
  `Idempotency-Key: rule_{rule_id}_user_{user_id}`
  PseudoGram recognizes the idempotency key and returns the previously generated `dm_id` instead of sending a second DM.

---

### 3. Upstream Platform Outage Beyond Max Retries (`MAX_RETRIES = 5`)
- **Condition**: PseudoGram experiences a prolonged platform outage returning continuous HTTP `500` errors or perpetual HTTP `429` blocks exceeding our 5 exponential backoff attempts.
- **Impact**: Once `retries > 5`, the system marks task status permanently as `'failed'` to prevent unresolvable tasks from clogging the persistent queue.
- **Limitation**: If PseudoGram recovers 2 hours later, our system will not automatically re-attempt sending that DM. (A manual dead-letter queue replay tool would be required in production).

---

### 4. Reconciliation Latency & Platform Status Delay
- **Condition**: PseudoGram accepts a DM (`202 Accepted`), but takes several minutes to transition its internal status from `queued` to `delivered` or `failed`.
- **Impact**: During this reconciliation delay, `GET /stats` counts the task under `queued` (pending reconciliation).
- **Limitation**: The `/stats` endpoint reflects `sent` count strictly when PseudoGram confirms delivery via `GET /v1/dm/{dm_id}`. If the platform reconciliation API suffers latency, the `sent` statistic update will be delayed accordingly.

---

### 5. Out-of-Order Comment Deletion Post Platform Delivery
- **Condition**: A user comments `PRICE`, the DM is accepted (`202`) and physically delivered to the user by Instagram, but 2 seconds later the user deletes their comment and `comment.deleted` webhook arrives.
- **Impact**: Because the mock Instagram platform has no "un-send DM" API endpoint, the DM remains delivered in the user's inbox. Our system records the deletion event in `deleted_comments`, but leaves the task status as `delivered` to reflect real-world state.

---

### 6. Render Free Tier Ephemeral Storage & Container Spin-Down Behavior
- **Condition**: Deployment on Render's Free Web Service tier (`plan: free`).
- **Limitation**: Render's Free Web Service tier uses an ephemeral filesystem and spins down containers after 15 minutes of inactivity.
- **Impact**: When the container spins down, restarts, or redeploys, the local SQLite database file (`linkplease.db`) resets. Previously created rules, event logs, and `/stats` history will clear upon container restart.
- **Production Path**: In a production environment with persistent requirements, SQLite can be mounted to a Render Persistent Disk volume or migrated to a managed cloud database such as PostgreSQL.

---

### 7. SQLite Concurrency & Scaling Limitations at Ultra-High Scale
- **Condition**: Scaling beyond single-instance deployment to multi-region worker clusters handling tens of millions of DMs per month.
- **Limitation**: SQLite WAL mode supports high-performance concurrent readers and serialized single-writer transactions. Under ultra-high write concurrency across multiple distributed container instances, SQLite file locking can become a bottleneck.
- **Production Path**: At scale, SQLite would be replaced with PostgreSQL (with `SELECT ... FOR UPDATE SKIP LOCKED`) or Redis Streams as the distributed persistent queue.
