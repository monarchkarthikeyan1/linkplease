import asyncio
import time
import httpx
import logging
from typing import List
from config import settings
import database as db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")

class RateLimiter:
    """
    Sliding window rate limiter: enforces max 10 requests per rolling 60 seconds window,
    with minimum interval pacing to strictly avoid breaching rate limits.
    """
    def __init__(self, max_requests: int = 10, window_seconds: float = 60.0, min_interval: float = 6.05):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.min_interval = min_interval
        self.timestamps: List[float] = []
        self.last_send_time: float = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            # Clean old timestamps outside rolling window
            self.timestamps = [t for t in self.timestamps if now - t < self.window_seconds]
            
            # Check sliding window count
            if len(self.timestamps) >= self.max_requests:
                sleep_needed = self.window_seconds - (now - self.timestamps[0]) + 0.1
                if sleep_needed > 0:
                    logger.info(f"[RateLimiter] Sliding window full ({len(self.timestamps)}/10). Sleeping {sleep_needed:.2f}s...")
                    await asyncio.sleep(sleep_needed)
                    now = time.monotonic()
                    self.timestamps = [t for t in self.timestamps if now - t < self.window_seconds]

            # Enforce minimum interval between sends
            elapsed = now - self.last_send_time
            if elapsed < self.min_interval:
                sleep_needed = self.min_interval - elapsed
                logger.info(f"[RateLimiter] Pacing sends. Sleeping {sleep_needed:.2f}s...")
                await asyncio.sleep(sleep_needed)
                now = time.monotonic()

            self.timestamps.append(now)
            self.last_send_time = now

rate_limiter = RateLimiter(
    max_requests=settings.RATE_LIMIT_MAX_REQUESTS,
    window_seconds=settings.RATE_LIMIT_WINDOW_SECONDS,
    min_interval=settings.MIN_SEND_INTERVAL_SECONDS
)

async def outbound_sender_loop():
    """
    Background worker loop that fetches pending DM tasks from SQLite and sends them to PseudoGram API.
    """
    logger.info("Outbound DM sender loop started.")
    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            try:
                task = db.get_next_pending_task()
                if not task:
                    await asyncio.sleep(settings.WORKER_POLL_INTERVAL)
                    continue

                task_id = task["id"]
                comment_id = task["comment_id"]
                user_id = task["user_id"]
                message = task["message"]
                idempotency_key = task["idempotency_key"]
                retries = task["retries"]

                # Check if comment was deleted
                if db.is_comment_deleted(comment_id):
                    logger.info(f"Comment {comment_id} was deleted. Cancelling task {task_id}.")
                    db.mark_task_failed(task_id, reason="comment_deleted")
                    continue

                # Acquire rate limiter slot
                await rate_limiter.acquire()

                url = f"{settings.PSEUDOGRAM_BASE_URL.rstrip('/')}/v1/dm/send"
                headers = {
                    "X-API-Key": settings.PSEUDOGRAM_API_KEY,
                    "Content-Type": "application/json",
                    "Idempotency-Key": idempotency_key
                }
                payload = {
                    "recipient_user_id": user_id,
                    "message": message,
                    "comment_id": comment_id
                }

                logger.info(f"Sending DM for task {task_id} (comment {comment_id}, user {user_id}, idempotency={idempotency_key})...")
                
                try:
                    response = await client.post(url, json=payload, headers=headers)
                except httpx.RequestError as net_err:
                    # Network error / lost connection: retry with SAME Idempotency-Key
                    new_retries = retries + 1
                    if new_retries > settings.MAX_RETRIES:
                        logger.error(f"Task {task_id} network error after max retries: {net_err}")
                        db.mark_task_failed(task_id, reason="network_max_retries")
                    else:
                        backoff = (2 ** new_retries) * 1.0
                        logger.warning(f"Task {task_id} network error. Retrying in {backoff:.1f}s (attempt {new_retries})...")
                        db.reschedule_task(task_id, retries=new_retries, delay_seconds=backoff, error_msg=str(net_err))
                    continue

                if response.status_code in (200, 202):
                    data = response.json()
                    dm_id = data.get("dm_id")
                    api_status = data.get("status", "queued")
                    db.mark_task_sent(task_id, dm_id=dm_id, api_status=api_status)
                    logger.info(f"Task {task_id} accepted by PseudoGram: dm_id={dm_id}, status={api_status}")
                elif response.status_code == 429:
                    retry_after_str = response.headers.get("Retry-After", "60")
                    try:
                        retry_after = float(retry_after_str)
                    except ValueError:
                        retry_after = 60.0
                    logger.warning(f"Task {task_id} received 429 Rate Limited. Retry-After: {retry_after}s.")
                    db.reschedule_task(task_id, retries=retries, delay_seconds=retry_after + 0.5, error_msg="rate_limited")
                    await asyncio.sleep(retry_after)
                elif response.status_code == 500:
                    new_retries = retries + 1
                    if new_retries > settings.MAX_RETRIES:
                        logger.error(f"Task {task_id} failed after {new_retries} 500 retries.")
                        db.mark_task_failed(task_id, reason="500_max_retries")
                    else:
                        backoff = (2 ** new_retries) * 1.0
                        logger.warning(f"Task {task_id} received 500. Retrying in {backoff:.1f}s (attempt {new_retries})...")
                        db.reschedule_task(task_id, retries=new_retries, delay_seconds=backoff, error_msg="500_internal_error")
                else:
                    # 400 or other permanent client errors
                    logger.error(f"Task {task_id} permanent HTTP {response.status_code} error: {response.text}")
                    db.mark_task_failed(task_id, reason=f"http_{response.status_code}")

            except asyncio.CancelledError:
                logger.info("Outbound DM sender loop cancelled.")
                break
            except Exception as e:
                logger.exception(f"Unexpected error in sender loop: {e}")
                await asyncio.sleep(2.0)

async def reconciliation_loop():
    """
    Background worker loop that reconciles sent DMs via GET /v1/dm/{dm_id}.
    Reads do NOT count against the 10 req/60s send rate limit.
    """
    logger.info("Reconciliation loop started.")
    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            try:
                tasks_to_check = db.get_tasks_to_reconcile()
                if not tasks_to_check:
                    await asyncio.sleep(settings.RECONCILIATION_INTERVAL)
                    continue

                for task in tasks_to_check:
                    task_id = task["id"]
                    dm_id = task["dm_id"]
                    if not dm_id:
                        continue

                    url = f"{settings.PSEUDOGRAM_BASE_URL.rstrip('/')}/v1/dm/{dm_id}"
                    headers = {"X-API-Key": settings.PSEUDOGRAM_API_KEY}
                    
                    try:
                        response = await client.get(url, headers=headers)
                        if response.status_code == 200:
                            data = response.json()
                            status = data.get("status")
                            if status in ("delivered", "failed"):
                                logger.info(f"Reconciled task {task_id} dm_id={dm_id}: new status={status}")
                                db.update_reconciled_status(task_id, status)
                    except Exception as err:
                        logger.warning(f"Reconciliation check failed for task {task_id} dm_id={dm_id}: {err}")

                await asyncio.sleep(settings.RECONCILIATION_INTERVAL)

            except asyncio.CancelledError:
                logger.info("Reconciliation loop cancelled.")
                break
            except Exception as e:
                logger.exception(f"Unexpected error in reconciliation loop: {e}")
                await asyncio.sleep(2.0)
