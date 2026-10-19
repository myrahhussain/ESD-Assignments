"""
gRPC service that reimplements the "submit a job" operation (the same
operation as POST /jobs) as a unary RPC, running in its own process.

It shares the same SQLite-backed job store (job_store.py) that app.py's
HTTP endpoint and background worker use -- this is the single source of
truth, not a wrapper around the HTTP endpoint. Logs go to the SAME
logs/app.log file app.py writes to, using the same job_id field, so
Kibana can correlate a job across both protocols.

Metrics: this process has its own Prometheus registry, so it exposes its
own /metrics endpoint on port 8001 (app.py's registry is a different
process and cannot see these). Duration is measured SERVER-SIDE, around
the handler, so it excludes network time between client and server.

Run this in its own terminal, alongside `python app.py`:

    python grpc_server.py
"""

import os
import time
import logging
from concurrent import futures

import grpc
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pythonjsonlogger import jsonlogger

import job_service_pb2 as pb
import job_service_pb2_grpc as pb_grpc
import job_store

SERVICE_NAME = "invoice_job_queue_grpc"
GRPC_PORT = 50051
METRICS_PORT = 8001

os.makedirs("logs", exist_ok=True)

logger = logging.getLogger("invoice_app_grpc")
logger.setLevel(logging.INFO)
log_handler = logging.FileHandler("logs/app.log")  # same log file as the HTTP app
formatter = jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
log_handler.setFormatter(formatter)
logger.addHandler(log_handler)

# --- Prometheus metrics (labels are bounded: method and status code only) ---
grpc_requests_total = Counter(
    "grpc_requests_total",
    "Total gRPC calls handled, by RPC method and final gRPC status code",
    ["method", "code"]
)

grpc_request_duration_seconds = Histogram(
    "grpc_request_duration_seconds",
    "Server-side time spent handling a gRPC call, in seconds",
    ["method"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5]
)

grpc_requests_in_flight = Gauge(
    "grpc_requests_in_flight",
    "Number of gRPC calls currently being handled",
    ["method"]
)

# Same name as the HTTP app's counter, so one query can sum across both
# processes. The "protocol" label separates them.
jobs_submitted_total = Counter(
    "jobs_submitted_total",
    "Total number of invoice jobs submitted",
    ["protocol"]
)

idempotent_replays_total = Counter(
    "idempotent_replays_total",
    "Submissions answered from an existing Idempotency-Key instead of creating a job",
    ["protocol"]
)


class JobServiceServicer(pb_grpc.JobServiceServicer):

    def SubmitJob(self, request, context):
        method = "SubmitJob"
        grpc_requests_in_flight.labels(method=method).inc()
        start = time.time()
        code = grpc.StatusCode.OK
        try:
            data = {
                "customer": request.customer,
                "items": [
                    {"name": item.name, "qty": item.qty, "price": item.price}
                    for item in request.items
                ]
            }

            validation_error = job_store.validate_job_payload(data)
            if validation_error:
                logger.warning(
                    "gRPC job submission rejected",
                    extra={
                        "app_service": SERVICE_NAME,
                        "event_type": "job_rejected",
                        "reason": validation_error
                    }
                )
                code = grpc.StatusCode.INVALID_ARGUMENT
                context.set_code(code)
                context.set_details(validation_error)
                return pb.SubmitJobResponse()

            idempotency_key = request.idempotency_key or None
            job_id, status, is_replay = job_store.create_job(
                data["customer"], data["items"], idempotency_key
            )

            if is_replay:
                idempotent_replays_total.labels(protocol="grpc").inc()
            else:
                jobs_submitted_total.labels(protocol="grpc").inc()

            logger.info(
                "gRPC idempotent replay" if is_replay else "Job submitted via gRPC",
                extra={
                    "app_service": SERVICE_NAME,
                    "job_id": job_id,
                    "event_type": "job_replayed" if is_replay else "job_submitted",
                    "customer": data["customer"],
                    "items": data["items"]
                }
            )

            return pb.SubmitJobResponse(
                job_id=job_id,
                status=status,
                idempotent_replayed=is_replay
            )
        finally:
            grpc_request_duration_seconds.labels(method=method).observe(time.time() - start)
            grpc_requests_total.labels(method=method, code=code.name).inc()
            grpc_requests_in_flight.labels(method=method).dec()


def serve():
    job_store.init_db()
    start_http_server(METRICS_PORT)  # Prometheus scrapes http://localhost:8001/metrics
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb_grpc.add_JobServiceServicer_to_server(JobServiceServicer(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    print(f"gRPC JobService listening on port {GRPC_PORT}...")
    print(f"Prometheus metrics on port {METRICS_PORT}...")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()