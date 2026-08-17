import pytest
import hmac
import hashlib
import os
import json
import time

# Set test environment
os.environ["DB_PATH"] = "test_linkplease.db"
os.environ["PSEUDOGRAM_API_KEY"] = "test_secret_key_for_unit_tests"
os.environ["VERIFY_WEBHOOK_SIGNATURE"] = "true"

from fastapi.testclient import TestClient
import database as db
from main import app

client = TestClient(app)

@pytest.fixture(autouse=True)
def setup_db():
    if os.path.exists("test_linkplease.db"):
        os.remove("test_linkplease.db")
    db.init_db()
    yield
    if os.path.exists("test_linkplease.db"):
        os.remove("test_linkplease.db")

def generate_signature(body_bytes: bytes, secret: str = os.environ["PSEUDOGRAM_API_KEY"]) -> str:
    return "sha256=" + hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()

def send_webhook(payload: dict, custom_headers: dict = None, custom_bytes: bytes = None) -> tuple[int, dict]:
    if custom_bytes is not None:
        raw_bytes = custom_bytes
    else:
        raw_bytes = json.dumps(payload, separators=(',', ':')).encode('utf-8')
        
    headers = {"Content-Type": "application/json"}
    if custom_headers is not None:
        headers.update(custom_headers)
    else:
        headers["X-PseudoGram-Signature"] = generate_signature(raw_bytes)

    res = client.post("/webhook", content=raw_bytes, headers=headers)
    time.sleep(0.05)
    return res.status_code, res.json()

# --- PHASE 1 TESTS: Signature Verification ---

def test_signature_valid():
    payload = {"event_id": "evt_sig_1", "event_type": "comment.created", "data": {"comment_id": "cmt_sig_1", "text": "hello", "from": {"user_id": "usr_sig_1"}}}
    status, _ = send_webhook(payload)
    assert status == 200

def test_signature_invalid():
    payload = {"event_id": "evt_sig_2", "event_type": "comment.created", "data": {"comment_id": "cmt_sig_2"}}
    status, _ = send_webhook(payload, custom_headers={"X-PseudoGram-Signature": "sha256=invalidhex123456"})
    assert status == 401

def test_signature_missing():
    payload = {"event_id": "evt_sig_3", "event_type": "comment.created", "data": {"comment_id": "cmt_sig_3"}}
    status, _ = send_webhook(payload, custom_headers={})
    assert status == 401

def test_signature_modified_body():
    body1 = b'{"event_id":"evt_sig_4","event_type":"comment.created","data":{"text":"test"}}'
    sig1 = generate_signature(body1)
    body_modified = b'{"event_id":"evt_sig_4","event_type":"comment.created","data":{"text":"TAMPERED"}}'
    
    res = client.post("/webhook", content=body_modified, headers={"Content-Type": "application/json", "X-PseudoGram-Signature": sig1})
    assert res.status_code == 401

# --- PHASE 2 TESTS: comment.deleted ---

def test_comment_deleted_before_dm_sent():
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list"})
    
    # 1. Comment created
    payload_created = {
        "event_id": "evt_del_pre_1",
        "event_type": "comment.created",
        "data": {"comment_id": "cmt_del_pre_1", "text": "PRICE please", "from": {"user_id": "usr_del_pre_1"}}
    }
    send_webhook(payload_created)
    assert db.get_stats()["queued"] == 1

    # 2. Comment deleted before DM sent
    payload_deleted = {
        "event_id": "evt_del_pre_2",
        "event_type": "comment.deleted",
        "data": {"comment_id": "cmt_del_pre_1"}
    }
    send_webhook(payload_deleted)
    
    stats = db.get_stats()
    assert stats["failed"] == 1
    assert stats["queued"] == 0
    assert stats["sent"] == 0

def test_comment_deleted_after_dm_delivered():
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list"})
    
    payload_created = {
        "event_id": "evt_del_post_1",
        "event_type": "comment.created",
        "data": {"comment_id": "cmt_del_post_1", "text": "PRICE please", "from": {"user_id": "usr_del_post_1"}}
    }
    send_webhook(payload_created)
    
    task = db.get_next_pending_task()
    db.mark_task_sent(task["id"], dm_id="dm_del_post_1", api_status="queued")
    db.update_reconciled_status(task["id"], "delivered")
    assert db.get_stats()["sent"] == 1

    # Delete comment after confirmed delivered
    payload_deleted = {
        "event_id": "evt_del_post_2",
        "event_type": "comment.deleted",
        "data": {"comment_id": "cmt_del_post_1"}
    }
    send_webhook(payload_deleted)
    
    # Should remain delivered, not unsent
    stats = db.get_stats()
    assert stats["sent"] == 1
    assert stats["failed"] == 0

# --- PHASE 6 TESTS: Duplicate & Rule Isolation ---

def test_rule_isolation_and_deduplication():
    # Rule 1: PRICE, Rule 2: DISCOUNT
    res1 = client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price info"})
    res2 = client.post("/rules", json={"keyword": "DISCOUNT", "dm_message": "Discount code"})
    rule1_id = res1.json()["rule_id"]
    rule2_id = res2.json()["rule_id"]

    # Same user comments for Rule 1 twice
    p1 = {"event_id": "evt_iso_1", "event_type": "comment.created", "data": {"comment_id": "cmt_iso_1", "text": "PRICE check", "from": {"user_id": "usr_iso"}}}
    p2 = {"event_id": "evt_iso_2", "event_type": "comment.created", "data": {"comment_id": "cmt_iso_2", "text": "PRICE list", "from": {"user_id": "usr_iso"}}}
    send_webhook(p1)
    send_webhook(p2)
    
    # Same user comments for Rule 2 once
    p3 = {"event_id": "evt_iso_3", "event_type": "comment.created", "data": {"comment_id": "cmt_iso_3", "text": "DISCOUNT code", "from": {"user_id": "usr_iso"}}}
    send_webhook(p3)

    stats = db.get_stats()
    assert stats["queued"] == 2 # 1 for PRICE, 1 for DISCOUNT
    assert stats["duplicates_blocked"] == 1 # 2nd PRICE comment blocked

# --- PHASE 7 TESTS: Crash Recovery & Persistence ---

def test_crash_recovery_stale_processing():
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price list"})
    db.process_comment_match("cmt_crash_1", "usr_crash_1", "rule_crash_1", "Price list")
    
    # Task picked up -> becomes 'processing'
    task = db.get_next_pending_task()
    assert task is not None
    assert db.get_next_pending_task() is None # currently processing
    
    # Simulate crash / timeout by backdating last_attempt_at in SQLite
    conn = db.get_db()
    with conn:
        conn.execute("UPDATE dm_tasks SET last_attempt_at = '2020-01-01T00:00:00.000000+00:00' WHERE id = ?", (task["id"],))
        
    # Recover stuck tasks
    db.recover_stuck_processing_tasks()
    
    # Task recovered and available to be picked up again
    recovered = db.get_next_pending_task()
    assert recovered is not None
    assert recovered["id"] == task["id"]

# --- PHASE 8 & 9 TESTS: Stats Accuracy & Error Resilience ---

def test_accurate_stats_aggregates():
    # 1 delivered, 1 failed, 1 queued, 1 blocked
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Price info"})
    
    # Task 1: Delivered
    db.process_comment_match("cmt_stat_1", "usr_stat_1", "rule_stat", "Price info")
    t1 = db.get_next_pending_task()
    db.mark_task_sent(t1["id"], "dm_s1", "delivered")
    
    # Task 2: Failed
    db.process_comment_match("cmt_stat_2", "usr_stat_2", "rule_stat", "Price info")
    t2 = db.get_next_pending_task()
    db.mark_task_failed(t2["id"], "400_invalid_request")
    
    # Task 3: Queued
    db.process_comment_match("cmt_stat_3", "usr_stat_3", "rule_stat", "Price info")
    
    # Task 4: Blocked duplicate
    res_block = db.process_comment_match("cmt_stat_4", "usr_stat_1", "rule_stat", "Price info")
    assert res_block == "blocked"

    stats = db.get_stats()
    assert stats["sent"] == 1
    assert stats["failed"] == 1
    assert stats["queued"] == 1
    assert stats["duplicates_blocked"] == 1
