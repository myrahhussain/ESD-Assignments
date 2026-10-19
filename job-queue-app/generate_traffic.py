"""
Repeatable traffic generator for Assignment 2, Part C.

Sends a fixed, deterministic mix of SUCCESS and FAILURE calls to BOTH the
HTTP API and the gRPC service so every Grafana panel has data to show.
Run it while app.py and grpc_server.py are both running:

    python generate_traffic.py            # 60 rounds, ~1 second apart (~1 minute)
    python generate_traffic.py 180        # 180 rounds (~3 minutes)

What each round does (every round, plus extra events on a fixed schedule):

  gRPC   every round : 1 invalid call               -> INVALID_ARGUMENT
         every 5th   : 1 valid call                 -> OK
         every 10th  : same idempotency key each time -> 1st creates, later ones are replays
  HTTP   every round : GET /jobs/<unknown id>       -> 404
         every 3rd   : POST /jobs with bad body     -> 400
         every 5th   : 1 valid POST /jobs           -> 201
         every 10th  : same idempotency key each time -> 1st is 201, later ones 200 (replay)
         round 15/45 : chaos "fail next 2" + 2 POSTs-> 500, 500 (then back to normal)
         round 25    : 7 rapid unskipped bad POSTs  -> first 5 are 400, then 429s

The X-Skip-Rate-Limit header is the same test-only header caller.py uses, so
the normal calls don't trip the rate limiter; only the round-25 burst does.
"""

import sys
import time
import uuid

import grpc
import requests

import job_service_pb2 as pb
import job_service_pb2_grpc as pb_grpc

HTTP_BASE = "http://127.0.0.1:5000"
GRPC_TARGET = "127.0.0.1:50051"

channel = grpc.insecure_channel(GRPC_TARGET)
stub = pb_grpc.JobServiceStub(channel)

VALID_ITEMS = [{"name": "Chocolate Eclairs", "qty": 2, "price": 6}]
SKIP = {"X-Skip-Rate-Limit": "true"}


def grpc_call(items, key=""):
    request = pb.SubmitJobRequest(
        customer="Traffic Test",
        items=[pb.JobItem(name=i["name"], qty=i["qty"], price=i["price"]) for i in items],
        idempotency_key=key
    )
    try:
        stub.SubmitJob(request, timeout=3)
        return "OK"
    except grpc.RpcError as e:
        return e.code().name


def http_post(body, headers=None):
    try:
        r = requests.post(f"{HTTP_BASE}/jobs", json=body, headers=headers or {}, timeout=10)
        return r.status_code
    except requests.RequestException as e:
        return f"ERR({type(e).__name__})"


def main(rounds):
    replay_key_grpc = str(uuid.uuid4())
    replay_key_http = str(uuid.uuid4())
    good_body = {"customer": "Traffic Test", "items": VALID_ITEMS}
    bad_body = {"customer": "Traffic Test", "items": []}

    print(f"Running {rounds} rounds against {HTTP_BASE} and {GRPC_TARGET} ...")
    for n in range(1, rounds + 1):
        results = []

        results.append("grpc-bad:" + grpc_call([]))

        if n % 5 == 0:
            results.append("grpc-ok:" + grpc_call(VALID_ITEMS))
        if n % 10 == 0:
            results.append("grpc-replay:" + grpc_call(VALID_ITEMS, replay_key_grpc))

        try:
            r = requests.get(f"{HTTP_BASE}/jobs/does-not-exist", timeout=10)
            results.append(f"http-404:{r.status_code}")
        except requests.RequestException as e:
            results.append(f"http-404:ERR({type(e).__name__})")

        if n % 3 == 0:
            results.append(f"http-bad:{http_post(bad_body, SKIP)}")
        if n % 5 == 0:
            results.append(f"http-ok:{http_post(good_body, SKIP)}")
        if n % 10 == 0:
            results.append(
                f"http-replay:{http_post(good_body, {**SKIP, 'Idempotency-Key': replay_key_http})}"
            )

        if n in (15, 45):
            requests.post(f"{HTTP_BASE}/chaos", json={"fail_next_n_requests": 2}, timeout=10)
            results.append(f"http-chaos:{http_post(good_body, SKIP)},{http_post(good_body, SKIP)}")

        if n == 25:
            burst = [http_post(bad_body) for _ in range(7)]  # no skip header -> rate limited
            results.append(f"http-burst:{burst}")

        print(f"round {n:>3}: " + "  ".join(str(x) for x in results))
        time.sleep(1)

    print("Done.")


if __name__ == "__main__":
    total = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    main(total)