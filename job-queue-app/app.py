from flask import Flask, request, jsonify
from prometheus_client import Counter, Gauge, Histogram, Summary, generate_latest, CONTENT_TYPE_LATEST
import threading
import time
import os
import random
import logging
from collections import defaultdict, deque
from pythonjsonlogger import jsonlogger
from fpdf import FPDF
from datetime import datetime

import job_store

app = Flask(__name__)

job_store.init_db()  # jobs.db is the shared source of truth for both HTTP and gRPC

os.makedirs("invoices", exist_ok=True)
os.makedirs("logs", exist_ok=True)

logger = logging.getLogger("invoice_app")
logger.setLevel(logging.INFO)

log_handler = logging.FileHandler("logs/app.log")
formatter = jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
log_handler.setFormatter(formatter)
logger.addHandler(log_handler)

SERVICE_NAME = "invoice_job_queue"

# --- Chaos / fault injection state ---
CHAOS_STATE = {
    "slow_every_nth": 0,
    "failure_rate": 0.0,
    "fail_next_n_requests": 0,      # makes the next N POST /jobs calls return 500 immediately
    "timeout_next_n_requests": 0    # makes the next N POST /jobs calls hang for 5s before responding
}
jobs_processed_count = 0
chaos_lock = threading.Lock()

# --- Rate limiting: POST /jobs is limited per client IP ---
RATE_LIMIT_MAX_REQUESTS = 5
RATE_LIMIT_WINDOW_SECONDS = 60
rate_limit_lock = threading.Lock()
request_timestamps = defaultdict(deque)  # client IP -> deque of request times within the window


def check_rate_limit(client_key):
    """Sliding-window rate limit. Returns (allowed: bool, retry_after_seconds: int)."""
    now = time.time()
    with rate_limit_lock:
        timestamps = request_timestamps[client_key]

        while timestamps and timestamps[0] <= now - RATE_LIMIT_WINDOW_SECONDS:
            timestamps.popleft()

        if len(timestamps) >= RATE_LIMIT_MAX_REQUESTS:
            retry_after = int(RATE_LIMIT_WINDOW_SECONDS - (now - timestamps[0])) + 1
            return False, retry_after

        timestamps.append(now)
        return True, 0


jobs_submitted_total = Counter(
    "jobs_submitted_total",
    "Total number of invoice jobs submitted via the HTTP API"
)

jobs_in_progress = Gauge(
    "jobs_in_progress",
    "Number of jobs currently pending or being processed, regardless of submission path"
)

job_processing_seconds = Histogram(
    "job_processing_seconds",
    "Time taken to process a job, in seconds"
)

invoice_file_size_kb = Summary(
    "invoice_file_size_kb",
    "Size of generated invoice PDF files in kilobytes"
)

invoice_total_value = Histogram(
    "invoice_total_value",
    "Total dollar value of generated invoices",
    buckets=[10, 50, 100, 500, 1000, 5000, 10000]
)

jobs_failed_total = Counter(
    "jobs_failed_total",
    "Total number of jobs that failed during processing"
)

# --- Cardinality explosion demo ---
cardinality_test_counter = Counter(
    "cardinality_test_requests_total",
    "Test counter demonstrating cardinality explosion when labeled with a unique ID per call",
    ["request_id"]
)

cardinality_test_safe_counter = Counter(
    "cardinality_test_safe_requests_total",
    "Safe version - no per-request label, single series regardless of call volume"
)


def error_response(status_code, code, message):
    """Consistent error envelope for every error this API returns."""
    return jsonify({"error": {"code": code, "message": message}}), status_code


def generate_invoice(job_id, customer, items):
    pdf = FPDF()
    pdf.add_page()

    pdf.set_font("Helvetica", "B", 20)
    pdf.cell(0, 12, "SHOP INVOICE", align="C", ln=True)
    pdf.ln(2)

    pdf.set_font("Helvetica", size=10)
    invoice_number = job_id.split("-")[0].upper()
    pdf.cell(0, 6, f"Invoice #: {invoice_number}", ln=True)
    pdf.cell(0, 6, f"Date: {datetime.now().strftime('%B %d, %Y')}", ln=True)
    pdf.ln(4)

    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 6, f"Customer: {customer}", ln=True)
    pdf.ln(6)

    col_widths = [80, 25, 35, 40]
    headers = ["Item", "Qty", "Price", "Total"]

    pdf.set_font("Helvetica", "B", 10)
    pdf.set_fill_color(230, 230, 230)
    for width, header in zip(col_widths, headers):
        align = "L" if header == "Item" else "R"
        pdf.cell(width, 8, header, border=1, align=align, fill=True)
    pdf.ln()

    pdf.set_font("Helvetica", size=10)
    total = 0
    for item in items:
        line_total = item["qty"] * item["price"]
        total += line_total
        pdf.cell(col_widths[0], 8, str(item["name"]), border=1)
        pdf.cell(col_widths[1], 8, str(item["qty"]), border=1, align="R")
        pdf.cell(col_widths[2], 8, f"${item['price']:.2f}", border=1, align="R")
        pdf.cell(col_widths[3], 8, f"${line_total:.2f}", border=1, align="R")
        pdf.ln()

    pdf.ln(4)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(sum(col_widths[:3]), 8, "Grand Total:", align="R")
    pdf.cell(col_widths[3], 8, f"${total:.2f}", border=1, align="R")
    pdf.ln()

    filename = f"invoices/invoice_{job_id}.pdf"
    pdf.output(filename)
    return filename, total


@app.route("/")
def home():
    return "This is Myrah's invoice job queue."


@app.route("/chaos", methods=["POST"])
def set_chaos():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return error_response(400, "invalid_request", "Request body must be a JSON object with at least one chaos field.")

    if "slow_every_nth" in data:
        if not isinstance(data["slow_every_nth"], int) or data["slow_every_nth"] < 0:
            return error_response(400, "invalid_field", "'slow_every_nth' must be a non-negative integer.")
        CHAOS_STATE["slow_every_nth"] = data["slow_every_nth"]

    if "failure_rate" in data:
        if not isinstance(data["failure_rate"], (int, float)) or not (0 <= data["failure_rate"] <= 1):
            return error_response(400, "invalid_field", "'failure_rate' must be a number between 0 and 1.")
        CHAOS_STATE["failure_rate"] = data["failure_rate"]

    if "fail_next_n_requests" in data:
        if not isinstance(data["fail_next_n_requests"], int) or data["fail_next_n_requests"] < 0:
            return error_response(400, "invalid_field", "'fail_next_n_requests' must be a non-negative integer.")
        CHAOS_STATE["fail_next_n_requests"] = data["fail_next_n_requests"]

    if "timeout_next_n_requests" in data:
        if not isinstance(data["timeout_next_n_requests"], int) or data["timeout_next_n_requests"] < 0:
            return error_response(400, "invalid_field", "'timeout_next_n_requests' must be a non-negative integer.")
        CHAOS_STATE["timeout_next_n_requests"] = data["timeout_next_n_requests"]

    return jsonify({"status": "ok", "chaos_state": CHAOS_STATE})


@app.route("/chaos", methods=["GET"])
def get_chaos():
    return jsonify(CHAOS_STATE)


@app.route("/cardinality-test")
def cardinality_test():
    import uuid
    request_id = str(uuid.uuid4())
    cardinality_test_counter.labels(request_id=request_id).inc()
    return jsonify({"request_id": request_id})


@app.route("/cardinality-test-safe")
def cardinality_test_safe():
    cardinality_test_safe_counter.inc()
    return jsonify({"status": "called"})


@app.route("/jobs", methods=["POST"])
def create_job():
    # The resilience demo (timeout/retry/circuit breaker) is tested separately from
    # rate limiting per the assignment, so it's allowed to skip the limiter via this
    # explicit test-only header. Real traffic without this header is always limited.
    if request.headers.get("X-Skip-Rate-Limit") != "true":
        client_key = request.remote_addr or "unknown"
        allowed, retry_after = check_rate_limit(client_key)
        if not allowed:
            response = jsonify({
                "error": {
                    "code": "rate_limited",
                    "message": f"Too many requests. Limit is {RATE_LIMIT_MAX_REQUESTS} per {RATE_LIMIT_WINDOW_SECONDS} seconds. Try again in {retry_after} seconds."
                }
            })
            response.status_code = 429
            response.headers["Retry-After"] = str(retry_after)
            return response

    # --- Request-level chaos injection (simulates the server itself misbehaving,
    #     as opposed to the background worker chaos which only affects job processing) ---
    with chaos_lock:
        should_delay = CHAOS_STATE["timeout_next_n_requests"] > 0
        if should_delay:
            CHAOS_STATE["timeout_next_n_requests"] -= 1

        should_fail = CHAOS_STATE["fail_next_n_requests"] > 0
        if should_fail:
            CHAOS_STATE["fail_next_n_requests"] -= 1

    if should_delay:
        time.sleep(5)

    if should_fail:
        return error_response(500, "internal_error", "Simulated transient server failure (chaos: fail_next_n_requests).")

    data = request.get_json(silent=True)

    validation_error = job_store.validate_job_payload(data)
    if validation_error:
        return error_response(400, "invalid_request", validation_error)

    idempotency_key = request.headers.get("Idempotency-Key")
    job_id, status, is_replay = job_store.create_job(data["customer"], data["items"], idempotency_key)

    if is_replay:
        response = jsonify({"job_id": job_id, "status": status})
        response.status_code = 200
        response.headers["Location"] = f"/jobs/{job_id}"
        response.headers["Idempotent-Replayed"] = "true"
        return response

    jobs_submitted_total.inc()

    logger.info(
        "Job submitted",
        extra={
            "app_service": SERVICE_NAME,
            "job_id": job_id,
            "event_type": "job_submitted",
            "customer": data["customer"],
            "items": data["items"]
        }
    )

    response = jsonify({"job_id": job_id, "status": status})
    response.status_code = 201
    response.headers["Location"] = f"/jobs/{job_id}"
    return response


@app.route("/jobs/<job_id>")
def get_job(job_id):
    job = job_store.get_job(job_id)
    if job is None:
        return error_response(404, "job_not_found", f"No job with id '{job_id}' exists.")

    result = {"status": job["status"], "customer": job["customer"], "items": job["items"]}
    if job["file"]:
        result["file"] = job["file"]
    return jsonify(result)


@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}


def worker_loop():
    global jobs_processed_count
    while True:
        pending = job_store.get_pending_jobs()

        for job in pending:
            job_id = job["job_id"]
            job_store.update_job_status(job_id, "in_progress")
            logger.info(
                "Job processing started",
                extra={"app_service": SERVICE_NAME, "job_id": job_id, "event_type": "job_started"}
            )

            start_time = time.time()

            with chaos_lock:
                jobs_processed_count += 1
                current_count = jobs_processed_count
                slow_n = CHAOS_STATE["slow_every_nth"]
                fail_rate = CHAOS_STATE["failure_rate"]

            if fail_rate > 0 and random.random() < fail_rate:
                time.sleep(1)
                job_store.update_job_status(job_id, "failed")
                jobs_failed_total.inc()
                logger.error(
                    "Job failed during processing",
                    extra={
                        "app_service": SERVICE_NAME,
                        "job_id": job_id,
                        "event_type": "job_failed",
                        "reason": "simulated_failure"
                    }
                )
                continue

            if slow_n > 0 and current_count % slow_n == 0:
                time.sleep(30)
            else:
                time.sleep(8)

            filename, invoice_total = generate_invoice(job_id, job["customer"], job["items"])

            duration = time.time() - start_time
            job_processing_seconds.observe(duration)

            file_size_kb = os.path.getsize(filename) / 1024
            invoice_file_size_kb.observe(file_size_kb)

            invoice_total_value.observe(invoice_total)

            job_store.update_job_status(job_id, "done", file=filename)

            logger.info(
                "Job completed successfully",
                extra={
                    "app_service": SERVICE_NAME,
                    "job_id": job_id,
                    "event_type": "job_completed",
                    "invoice_file": filename,
                    "duration_seconds": duration,
                    "invoice_total": invoice_total
                }
            )

        # jobs_in_progress reflects the TRUE current count from the shared database,
        # so it stays correct regardless of whether jobs arrived via HTTP or gRPC.
        jobs_in_progress.set(job_store.count_jobs_in_progress())

        time.sleep(1)


if __name__ == "__main__":
    worker_thread = threading.Thread(target=worker_loop, daemon=True)
    worker_thread.start()
    app.run(debug=True, use_reloader=False)