import asyncio
import hmac
import hashlib
import uuid
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, status, Header
from fastapi.responses import JSONResponse

from config import settings
import database as db
from models import RuleCreate, RuleResponse, StatsResponse
from worker import outbound_sender_loop, reconciliation_loop

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    logger.info("Database initialized.")

    sender_task = asyncio.create_task(outbound_sender_loop())
    reconcile_task = asyncio.create_task(reconciliation_loop())
    logger.info("Background workers spawned.")

    yield

    sender_task.cancel()
    reconcile_task.cancel()
    await asyncio.gather(sender_task, reconcile_task, return_exceptions=True)
    logger.info("Background workers shut down.")

app = FastAPI(title="LinkPlease Instagram Automation", lifespan=lifespan)

@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    return db.get_stats()

@app.post("/rules", response_model=RuleResponse, status_code=status.HTTP_201_CREATED)
async def create_rule(rule_req: RuleCreate):
    rule_id = f"rule_{uuid.uuid4().hex[:10]}"
    try:
        res = db.add_rule(rule_id=rule_id, keyword=rule_req.keyword, dm_message=rule_req.dm_message)
        return RuleResponse(**res)
    except Exception as e:
        logger.error(f"Error creating rule: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/reset-test-database")
async def reset_database():
    conn = db.get_db()
    with conn:
        conn.execute("DELETE FROM user_rule_matches;")
        conn.execute("DELETE FROM blocked_matches;")
        conn.execute("DELETE FROM dm_tasks;")
        conn.execute("DELETE FROM events;")
        conn.execute("DELETE FROM rules;")
    return {"status": "reset complete"}

@app.post("/webhook")
async def handle_webhook(request: Request, x_pseudogram_signature: str = Header(None, alias="X-PseudoGram-Signature")):
    raw_body = await request.body()
    
    # HMAC-SHA256 Webhook Signature Verification (Phase 1)
    if settings.VERIFY_WEBHOOK_SIGNATURE:
        if not x_pseudogram_signature:
            logger.warning(f"Rejected webhook: Missing X-PseudoGram-Signature header. Headers: {dict(request.headers)}")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing signature header")

        if not x_pseudogram_signature.startswith("sha256="):
            logger.warning(f"Rejected webhook: Malformed X-PseudoGram-Signature header format: {x_pseudogram_signature}")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Malformed signature header")

        expected_sig = "sha256=" + hmac.new(settings.API_KEY.encode('utf-8'), raw_body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(x_pseudogram_signature, expected_sig):
            logger.warning(f"Rejected webhook mismatch. Recv: {x_pseudogram_signature} | Expected: {expected_sig} | KeyLen: {len(settings.API_KEY)}")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid signature")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    event_id = payload.get("event_id")
    event_type = payload.get("event_type")

    if not event_id or not event_type:
        raise HTTPException(status_code=400, detail="Missing event_id or event_type")

    # 1. Event Deduplication
    if db.is_event_duplicate(event_id):
        logger.info(f"Duplicate event {event_id} ignored.")
        return {"status": "ok", "message": "duplicate event ignored"}

    # Handle comment.deleted (Phase 2)
    if event_type == "comment.deleted":
        comment_id = payload.get("data", {}).get("comment_id")
        if comment_id:
            db.record_comment_deleted(comment_id)
            logger.info(f"Recorded comment.deleted for comment_id {comment_id}")
        return {"status": "ok", "message": "comment.deleted processed"}

    # Handle comment.created (Part A / Phase 2)
    if event_type == "comment.created":
        data = payload.get("data", {})
        comment_id = data.get("comment_id")
        comment_text = data.get("text", "")
        from_user = data.get("from", {})
        user_id = from_user.get("user_id")

        if not comment_id or not user_id:
            logger.warning("Malformed comment.created payload missing comment_id or user_id")
            return {"status": "ok", "message": "ignored malformed data"}

        # Background processing: Async task enqueue
        asyncio.create_task(process_webhook_event(comment_id, user_id, comment_text))
        return {"status": "ok", "message": "event queued for processing"}

    return {"status": "ok", "message": "unhandled event_type ignored"}

async def process_webhook_event(comment_id: str, user_id: str, comment_text: str):
    """Keyword matching & delivery deduplication task."""
    try:
        rules = db.get_all_rules()
        comment_lower = comment_text.lower()

        for rule in rules:
            rule_id = rule["rule_id"]
            keyword = rule["keyword"]
            dm_message = rule["dm_message"]

            if keyword in comment_lower:
                logger.info(f"Comment {comment_id} from {user_id} matched rule {rule_id} ('{keyword}')")
                db.process_comment_match(
                    comment_id=comment_id,
                    user_id=user_id,
                    rule_id=rule_id,
                    dm_message=dm_message
                )
    except Exception as e:
        logger.error(f"Error processing webhook event comment={comment_id}: {e}")
