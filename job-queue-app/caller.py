"""
A small "calling service" for the Invoice Job Queue API.

This script plays the role of ANOTHER service that depends on this API.
It demonstrates, against the real running Flask app:

  - a per-request timeout
  - a limited retry with exponential backoff for transient failures
    (only retried because each retry reuses the same Idempotency-Key,
    so repeating the request is safe and will not create a duplicate job)
  - a circuit breaker that stops calling after repeated failures and
    automatically tries again after a recovery period

Run this while app.py is running in another terminal:

    pip install requests
    python caller.py
"""

import time
import uuid
import requests

BASE_URL = "http://127.0.0.1:5000"

# --- Caller settings (stated here for the report) ---
TIMEOUT_SECONDS = 2
MAX_ATTEMPTS = 3                 # 1 initial attempt + up to 2 retries
BACKOFF_BASE_SECONDS = 1         # backoff sequence: 1s, then 2s
CIRCUIT_BREAKER_FAILURE_THRESHOLD = 2   # consecutive FULLY-FAILED submissions before opening
CIRCUIT_BREAKER_RECOVERY_SECONDS = 10


def reset_chaos():
    requests.post(f"{BASE_URL}/chaos", json={
        "fail_next_n_requests": 0,
        "timeout_next_n_requests": 0
    })


def set_chaos(**kwargs):
    requests.post(f"{BASE_URL}/chaos", json=kwargs)


class CircuitBreaker:
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

    def __init__(self, failure_threshold, recovery_seconds):
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds
        self.state = self.CLOSED
        self.consecutive_failures = 0
        self.opened_at = None

    def before_call(self):
        if self.state == self.OPEN:
            elapsed = time.time() - self.opened_at
            if elapsed >= self.recovery_seconds:
                self.state = self.HALF_OPEN
                print(f"  [breaker] recovery period elapsed -> HALF_OPEN, allowing one trial call")
                return True
            else:
                remaining = self.recovery_seconds - elapsed
                print(f"  [breaker] OPEN -> blocking call without contacting the server "
                      f"({remaining:.1f}s left until a trial call is allowed)")
                return False
        return True

    def record_success(self):
        if self.state == self.HALF_OPEN:
            print("  [breaker] trial call succeeded -> closing circuit")
        self.state = self.CLOSED
        self.consecutive_failures = 0
        self.opened_at = None

    def record_failure(self):
        self.consecutive_failures += 1
        if self.state == self.HALF_OPEN:
            print("  [breaker] trial call failed -> reopening circuit")
            self.state = self.OPEN
            self.opened_at = time.time()
        elif self.consecutive_failures >= self.failure_threshold:
            print(f"  [breaker] {self.consecutive_failures} consecutive failed submissions -> opening circuit")
            self.state = self.OPEN
            self.opened_at = time.time()


def submit_job(breaker, customer, items):
    """Submits one logical job. Retries transient failures with backoff,
    using the same Idempotency-Key each retry so repeating is safe."""

    if not breaker.before_call():
        return None, "circuit_open"

    idempotency_key = str(uuid.uuid4())

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            print(f"  attempt {attempt}/{MAX_ATTEMPTS}: POST /jobs (timeout={TIMEOUT_SECONDS}s)")
            resp = requests.post(
                f"{BASE_URL}/jobs",
                json={"customer": customer, "items": items},
                headers={
                    "Idempotency-Key": idempotency_key,
                    "X-Skip-Rate-Limit": "true"  # this demo tests timeout/retry/breaker, not rate limiting
                },
                timeout=TIMEOUT_SECONDS
            )
        except requests.exceptions.Timeout:
            print(f"    -> timed out after {TIMEOUT_SECONDS}s")
            if attempt < MAX_ATTEMPTS:
                backoff = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                print(f"    -> retrying in {backoff}s (safe: same Idempotency-Key: {idempotency_key[:8]}...)")
                time.sleep(backoff)
                continue
            breaker.record_failure()
            return None, "timeout"

        if resp.status_code in (200, 201):
            print(f"    -> success ({resp.status_code}): {resp.json()}")
            breaker.record_success()
            return resp, None

        if resp.status_code >= 500:
            print(f"    -> server error {resp.status_code}: {resp.json()}")
            if attempt < MAX_ATTEMPTS:
                backoff = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                print(f"    -> retrying in {backoff}s (safe: same Idempotency-Key: {idempotency_key[:8]}...)")
                time.sleep(backoff)
                continue
            breaker.record_failure()
            return resp, "server_error"

        # 4xx is a client-side problem, not transient -- do not retry
        print(f"    -> client error {resp.status_code}, not retrying: {resp.json()}")
        return resp, "client_error"

    breaker.record_failure()
    return None, "exhausted_retries"


def main():
    print(f"Caller settings: timeout={TIMEOUT_SECONDS}s, max_attempts={MAX_ATTEMPTS}, "
          f"backoff_base={BACKOFF_BASE_SECONDS}s, "
          f"breaker_threshold={CIRCUIT_BREAKER_FAILURE_THRESHOLD} consecutive failures, "
          f"breaker_recovery={CIRCUIT_BREAKER_RECOVERY_SECONDS}s")
    print()

    breaker = CircuitBreaker(CIRCUIT_BREAKER_FAILURE_THRESHOLD, CIRCUIT_BREAKER_RECOVERY_SECONDS)
    reset_chaos()

    print("=" * 70)
    print("PHASE 1: normal operation")
    print("=" * 70)
    submit_job(breaker, "Phase1 Customer", [{"name": "Macaron", "qty": 1, "price": 3}])

    print()
    print("=" * 70)
    print("PHASE 2: server hangs once (timeout), then succeeds on retry")
    print("=" * 70)
    set_chaos(timeout_next_n_requests=1)
    submit_job(breaker, "Phase2 Customer", [{"name": "Eclair", "qty": 1, "price": 6}])

    print()
    print("=" * 70)
    print("PHASE 3: repeated server failures trip the circuit breaker open")
    print("=" * 70)
    set_chaos(fail_next_n_requests=20)

    print("-- submission A (will exhaust retries and fail) --")
    submit_job(breaker, "Phase3 Customer A", [{"name": "Cupcake", "qty": 1, "price": 4}])

    print("-- submission B (will exhaust retries, failure threshold reached, breaker opens) --")
    submit_job(breaker, "Phase3 Customer B", [{"name": "Cupcake", "qty": 1, "price": 4}])

    print("-- submission C (breaker is now OPEN, should be blocked without calling the server) --")
    submit_job(breaker, "Phase3 Customer C", [{"name": "Cupcake", "qty": 1, "price": 4}])

    print()
    print("=" * 70)
    print(f"PHASE 4: waiting {CIRCUIT_BREAKER_RECOVERY_SECONDS}s for the breaker's recovery period, "
          f"then clearing chaos and retrying")
    print("=" * 70)
    time.sleep(CIRCUIT_BREAKER_RECOVERY_SECONDS + 1)
    reset_chaos()
    submit_job(breaker, "Phase4 Customer", [{"name": "Tiramisu Slice", "qty": 1, "price": 15}])

    print()
    print("Done.")


if __name__ == "__main__":
    main()
