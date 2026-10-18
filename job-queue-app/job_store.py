"""
The single shared source of truth for job data.

Both app.py (HTTP, running in one process) and grpc_server.py (gRPC,
running in a separate process) import this module and call these same
functions directly. Neither one wraps the other -- they both go through
this same business logic and the same on-disk database (jobs.db).

This replaces Assignment 1's in-memory `jobs = {}` dict, which could not
be shared across two separate processes.
"""

import sqlite3
import json
import threading
import uuid

DB_PATH = "jobs.db"
_db_lock = threading.Lock()


def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                customer TEXT NOT NULL,
                items TEXT NOT NULL,
                status TEXT NOT NULL,
                file TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS idempotency_keys (
                idempotency_key TEXT PRIMARY KEY,
                job_id TEXT NOT NULL
            )
        """)
        conn.commit()


def validate_job_payload(data):
    """Returns an error message string if invalid, else None.
    Shared by both the HTTP endpoint and the gRPC handler."""
    if not isinstance(data, dict):
        return "Request body must be a JSON object."
    if "customer" not in data or not isinstance(data["customer"], str) or not data["customer"].strip():
        return "Field 'customer' is required and must be a non-empty string."
    if "items" not in data or not isinstance(data["items"], list) or len(data["items"]) == 0:
        return "Field 'items' is required and must be a non-empty list."
    for item in data["items"]:
        if not isinstance(item, dict):
            return "Each item must be an object with 'name', 'qty', and 'price'."
        if "name" not in item or not isinstance(item["name"], str) or not item["name"].strip():
            return "Each item needs a non-empty 'name'."
        if "qty" not in item or not isinstance(item["qty"], (int, float)) or item["qty"] <= 0:
            return "Each item needs a positive numeric 'qty'."
        if "price" not in item or not isinstance(item["price"], (int, float)) or item["price"] < 0:
            return "Each item needs a non-negative numeric 'price'."
    return None


def create_job(customer, items, idempotency_key=None):
    """
    Creates a job, or returns the existing one if idempotency_key was
    already used. Returns (job_id, status, is_replay).
    """
    with _db_lock:
        with get_connection() as conn:
            if idempotency_key:
                row = conn.execute(
                    "SELECT job_id FROM idempotency_keys WHERE idempotency_key = ?",
                    (idempotency_key,)
                ).fetchone()
                if row:
                    existing = conn.execute(
                        "SELECT status FROM jobs WHERE job_id = ?", (row["job_id"],)
                    ).fetchone()
                    return row["job_id"], existing["status"], True

            job_id = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO jobs (job_id, customer, items, status, file) VALUES (?, ?, ?, ?, ?)",
                (job_id, customer, json.dumps(items), "pending", None)
            )
            if idempotency_key:
                conn.execute(
                    "INSERT INTO idempotency_keys (idempotency_key, job_id) VALUES (?, ?)",
                    (idempotency_key, job_id)
                )
            conn.commit()
            return job_id, "pending", False


def get_job(job_id):
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        return {
            "job_id": row["job_id"],
            "customer": row["customer"],
            "items": json.loads(row["items"]),
            "status": row["status"],
            "file": row["file"]
        }


def get_pending_jobs():
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM jobs WHERE status = 'pending'").fetchall()
        return [
            {
                "job_id": r["job_id"],
                "customer": r["customer"],
                "items": json.loads(r["items"])
            }
            for r in rows
        ]


def update_job_status(job_id, status, file=None):
    with _db_lock:
        with get_connection() as conn:
            if file is not None:
                conn.execute("UPDATE jobs SET status = ?, file = ? WHERE job_id = ?", (status, file, job_id))
            else:
                conn.execute("UPDATE jobs SET status = ? WHERE job_id = ?", (status, job_id))
            conn.commit()


def count_jobs_in_progress():
    """True current count of pending+in_progress jobs, regardless of
    whether they were submitted via HTTP or gRPC. Used to drive the
    jobs_in_progress Gauge accurately instead of manual inc()/dec()
    bookkeeping, which would drift once two separate processes can
    create jobs that a single worker later finishes."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM jobs WHERE status IN ('pending', 'in_progress')"
        ).fetchone()
        return row["cnt"]
