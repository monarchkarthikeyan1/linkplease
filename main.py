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

@app.post("/rules", response_model=RuleResponse, status_code=status.HTTP_201_CREATED)
async def create_rule(rule_req: RuleCreate):
    rule_id = f"rule_{uuid.uuid4().hex[:10]}"
    try:
        res = db.add_rule(rule_id=rule_id, keyword=rule_req.keyword, dm_message=rule_req.dm_message)
        return RuleResponse(**res)
    except Exception as e:
        logger.error(f"Error creating rule: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/webhook")
async def handle_webhook(request: Request, x_pseudogram_signature: str = Header(None, alias="X-PseudoGram-Signature")):
    raw_body = await request.body()
    
    # HMAC-SHA256 Webhook Signature Verification (Phase 1)
    if settings.VERIFY_WEBHOOK_SIGNATURE:
        if not x_pseudogram_signature:
            logger.warning("Rejected webhook: Missing X-PseudoGram-Signature header.")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing signature header")

        if not x_pseudogram_signature.startswith("sha256="):
            logger.warning("Rejected webhook: Malformed X-PseudoGram-Signature header format.")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Malformed signature header")

        expected_sig = "sha256=" + hmac.new(settings.API_KEY.encode('utf-8'), raw_body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(x_pseudogram_signature, expected_sig):
            logger.warning("Rejected webhook: Invalid HMAC signature mismatch.")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid signature")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    event_id = payload.get("event_id")
    event_type = payload.get("event_type")
    data = payload.get("data", {})

    if not event_id or not event_type:
        raise HTTPException(status_code=400, detail="Missing required event_id or event_type")

    # Fast event deduplication (Database-level UNIQUE constraint on event_id)
    if db.is_event_duplicate(event_id):
        return JSONResponse(status_code=200, content={"status": "ok", "message": "duplicate event ignored"})

    # Asynchronously process event in background to ensure webhook returns HTTP 200 in < 5ms
    asyncio.create_task(process_webhook_event(event_type, data))

    return JSONResponse(status_code=200, content={"status": "ok"})

async def process_webhook_event(event_type: str, data: dict):
    comment_id = data.get("comment_id")
    if not comment_id:
        return

    # Phase 2: Handle comment.deleted events
    if event_type == "comment.deleted":
        logger.info(f"Processing comment.deleted for comment_id={comment_id}")
        db.record_comment_deleted(comment_id)
        return

    if event_type == "comment.created":
        text = data.get("text", "")
        from_user = data.get("from", {})
        user_id = from_user.get("user_id") if isinstance(from_user, dict) else None

        if not text or not user_id:
            return

        text_lower = text.lower()
        rules = db.get_all_rules()

        for rule in rules:
            rule_id = rule["rule_id"]
            keyword = rule["keyword"].lower()
            dm_message = rule["dm_message"]

            if keyword in text_lower:
                logger.info(f"Comment {comment_id} from {user_id} matched rule {rule_id} ('{keyword}')")
                result = db.process_comment_match(
                    comment_id=comment_id,
                    user_id=user_id,
                    rule_id=rule_id,
                    dm_message=dm_message
                )
                logger.info(f"Match result for comment {comment_id}: {result}")

@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    stats_data = db.get_stats()
    return StatsResponse(**stats_data)
