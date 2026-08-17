import sqlite3
import datetime
from typing import List, Dict, Any, Optional
from config import settings

def get_utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def get_db():
    conn = sqlite3.connect(settings.DB_PATH, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    with conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        
        # Rules table
        conn.execute("""
        CREATE TABLE IF NOT EXISTS rules (
            rule_id TEXT PRIMARY KEY,
            keyword TEXT NOT NULL,
            dm_message TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # Deduplicate event_ids
        conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # Deleted comments tracking
        conn.execute("""
        CREATE TABLE IF NOT EXISTS deleted_comments (
            comment_id TEXT PRIMARY KEY,
            deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # Unique user-rule pairs sent or queued
        conn.execute("""
        CREATE TABLE IF NOT EXISTS user_rule_matches (
            user_id TEXT NOT NULL,
            rule_id TEXT NOT NULL,
            comment_id TEXT NOT NULL,
            matched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, rule_id)
        );
        """)
        
        # Blocked matches (duplicate user-rule attempts)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS blocked_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            rule_id TEXT NOT NULL,
            comment_id TEXT NOT NULL,
            blocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # Persistent DM deliveries queue & state tracking
        conn.execute("""
        CREATE TABLE IF NOT EXISTS dm_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            comment_id TEXT UNIQUE NOT NULL,
            user_id TEXT NOT NULL,
            rule_id TEXT NOT NULL,
            message TEXT NOT NULL,
            idempotency_key TEXT UNIQUE,
            status TEXT NOT NULL DEFAULT 'queued',
            dm_id TEXT,
            api_status TEXT,
            retries INTEGER DEFAULT 0,
            next_retry_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_attempt_at TIMESTAMP,
            last_error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Column migrations if table existed prior to schema update
        table_info = [col[1] for col in conn.execute("PRAGMA table_info(dm_tasks)").fetchall()]
        if "idempotency_key" not in table_info:
            conn.execute("ALTER TABLE dm_tasks ADD COLUMN idempotency_key TEXT;")
        if "last_attempt_at" not in table_info:
            conn.execute("ALTER TABLE dm_tasks ADD COLUMN last_attempt_at TIMESTAMP;")
        if "last_error" not in table_info:
            conn.execute("ALTER TABLE dm_tasks ADD COLUMN last_error TEXT;")
        
        # Create indexes for fast worker & reconciliation queries
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dm_tasks_status ON dm_tasks(status, next_retry_at);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dm_tasks_reconcile ON dm_tasks(status, api_status);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dm_tasks_comment ON dm_tasks(comment_id);")

def is_event_duplicate(event_id: str) -> bool:
    conn = get_db()
    with conn:
        cursor = conn.execute("INSERT OR IGNORE INTO events (event_id) VALUES (?)", (event_id,))
        return cursor.rowcount == 0

def add_rule(rule_id: str, keyword: str, dm_message: str) -> dict:
    conn = get_db()
    keyword_clean = keyword.strip().lower()
    with conn:
        conn.execute(
            "INSERT INTO rules (rule_id, keyword, dm_message) VALUES (?, ?, ?)",
            (rule_id, keyword_clean, dm_message)
        )
    return {"rule_id": rule_id, "keyword": keyword, "dm_message": dm_message}

def get_all_rules() -> List[dict]:
    conn = get_db()
    with conn:
        rows = conn.execute("SELECT rule_id, keyword, dm_message FROM rules").fetchall()
        return [dict(r) for r in rows]

def is_comment_deleted(comment_id: str) -> bool:
    conn = get_db()
    with conn:
        row = conn.execute("SELECT 1 FROM deleted_comments WHERE comment_id = ?", (comment_id,)).fetchone()
        return row is not None

def record_comment_deleted(comment_id: str):
    conn = get_db()
    now = get_utc_now()
    with conn:
        conn.execute("INSERT OR IGNORE INTO deleted_comments (comment_id) VALUES (?)", (comment_id,))
        conn.execute(
            """
            UPDATE dm_tasks 
            SET status = 'failed', api_status = 'failed', last_error = 'comment_deleted', updated_at = ? 
            WHERE comment_id = ? AND (api_status IS NULL OR api_status != 'delivered')
            """,
            (now, comment_id)
        )

def process_comment_match(comment_id: str, user_id: str, rule_id: str, dm_message: str) -> str:
    conn = get_db()
    now = get_utc_now()
    idempotency_key = f"rule_{rule_id}_user_{user_id}"
    with conn:
        # Check if comment was already deleted
        del_row = conn.execute("SELECT 1 FROM deleted_comments WHERE comment_id = ?", (comment_id,)).fetchone()
        if del_row:
            return "skipped_deleted"
            
        try:
            conn.execute(
                "INSERT INTO user_rule_matches (user_id, rule_id, comment_id, matched_at) VALUES (?, ?, ?, ?)",
                (user_id, rule_id, comment_id, now)
            )
        except sqlite3.IntegrityError:
            # User already matched this rule -> Block duplicate
            conn.execute(
                "INSERT INTO blocked_matches (user_id, rule_id, comment_id, blocked_at) VALUES (?, ?, ?, ?)",
                (user_id, rule_id, comment_id, now)
            )
            return "blocked"
            
        # Insert delivery task into SQLite queue
        conn.execute(
            """
            INSERT OR IGNORE INTO dm_tasks 
            (comment_id, user_id, rule_id, message, idempotency_key, status, next_retry_at, created_at, updated_at) 
            VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?)
            """,
            (comment_id, user_id, rule_id, dm_message, idempotency_key, now, now, now)
        )
        return "queued"

def recover_stuck_processing_tasks():
    """Crash recovery: reset tasks stuck in 'processing' longer than timeout back to 'queued'."""
    conn = get_db()
    now_dt = datetime.datetime.now(datetime.timezone.utc)
    cutoff = (now_dt - datetime.timedelta(seconds=settings.PROCESSING_TIMEOUT_SECONDS)).isoformat()
    now_str = now_dt.isoformat()
    with conn:
        conn.execute(
            """
            UPDATE dm_tasks 
            SET status = 'queued', next_retry_at = ?, updated_at = ? 
            WHERE status = 'processing' AND (last_attempt_at IS NULL OR last_attempt_at <= ?)
            """,
            (now_str, now_str, cutoff)
        )

def get_next_pending_task() -> Optional[dict]:
    conn = get_db()
    now = get_utc_now()
    with conn:
        recover_stuck_processing_tasks()
        row = conn.execute(
            """
            SELECT id, comment_id, user_id, rule_id, message, idempotency_key, dm_id, retries 
            FROM dm_tasks 
            WHERE status = 'queued' AND next_retry_at <= ? 
            ORDER BY id ASC LIMIT 1
            """,
            (now,)
        ).fetchone()
        
        if row:
            task = dict(row)
            # Atomically mark as processing
            conn.execute(
                "UPDATE dm_tasks SET status = 'processing', last_attempt_at = ?, updated_at = ? WHERE id = ?",
                (now, now, task["id"])
            )
            return task
        return None

def mark_task_sent(task_id: int, dm_id: str, api_status: str):
    conn = get_db()
    now = get_utc_now()
    with conn:
        conn.execute(
            """
            UPDATE dm_tasks 
            SET status = 'sent', dm_id = ?, api_status = ?, updated_at = ? 
            WHERE id = ?
            """,
            (dm_id, api_status, now, task_id)
        )

def mark_task_failed(task_id: int, reason: str = "failed"):
    conn = get_db()
    now = get_utc_now()
    with conn:
        conn.execute(
            """
            UPDATE dm_tasks 
            SET status = 'failed', api_status = 'failed', last_error = ?, updated_at = ? 
            WHERE id = ?
            """,
            (reason, now, task_id)
        )

def reschedule_task(task_id: int, retries: int, delay_seconds: float, error_msg: str = ""):
    conn = get_db()
    next_time = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=delay_seconds)).isoformat()
    now = get_utc_now()
    with conn:
        conn.execute(
            """
            UPDATE dm_tasks 
            SET status = 'queued', retries = ?, next_retry_at = ?, last_error = ?, updated_at = ? 
            WHERE id = ?
            """,
            (retries, next_time, error_msg, now, task_id)
        )

def get_tasks_to_reconcile() -> List[dict]:
    conn = get_db()
    with conn:
        rows = conn.execute(
            """
            SELECT id, comment_id, user_id, rule_id, dm_id, retries 
            FROM dm_tasks 
            WHERE status = 'sent' AND api_status = 'queued' AND dm_id IS NOT NULL
            """
        ).fetchall()
        return [dict(r) for r in rows]

def update_reconciled_status(task_id: int, new_api_status: str):
    conn = get_db()
    now = get_utc_now()
    with conn:
        if new_api_status == "delivered":
            conn.execute(
                "UPDATE dm_tasks SET api_status = 'delivered', updated_at = ? WHERE id = ?",
                (now, task_id)
            )
        elif new_api_status == "failed":
            row = conn.execute("SELECT retries FROM dm_tasks WHERE id = ?", (task_id,)).fetchone()
            retries = row["retries"] if row else 0
            if retries < settings.MAX_RETRIES:
                new_retries = retries + 1
                delay = (2 ** new_retries) * 1.0
                next_time = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=delay)).isoformat()
                conn.execute(
                    """
                    UPDATE dm_tasks 
                    SET status = 'queued', api_status = NULL, retries = ?, next_retry_at = ?, last_error = 'reconciliation_failed', updated_at = ? 
                    WHERE id = ?
                    """,
                    (new_retries, next_time, now, task_id)
                )
            else:
                conn.execute(
                    "UPDATE dm_tasks SET status = 'failed', api_status = 'failed', last_error = 'reconciliation_max_retries', updated_at = ? WHERE id = ?",
                    (now, task_id)
                )

def get_stats() -> dict:
    conn = get_db()
    with conn:
        sent_count = conn.execute(
            "SELECT COUNT(*) FROM dm_tasks WHERE api_status = 'delivered'"
        ).fetchone()[0]
        
        failed_count = conn.execute(
            "SELECT COUNT(*) FROM dm_tasks WHERE status = 'failed'"
        ).fetchone()[0]
        
        queued_count = conn.execute(
            """
            SELECT COUNT(*) FROM dm_tasks 
            WHERE status IN ('queued', 'processing') OR (status = 'sent' AND (api_status = 'queued' OR api_status IS NULL))
            """
        ).fetchone()[0]
        
        blocked_count = conn.execute(
            "SELECT COUNT(*) FROM blocked_matches"
        ).fetchone()[0]
        
        return {
            "sent": sent_count,
            "failed": failed_count,
            "queued": queued_count,
            "duplicates_blocked": blocked_count
        }
