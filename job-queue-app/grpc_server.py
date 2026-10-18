"""
gRPC service that reimplements the "submit a job" operation (the same
operation as POST /jobs) as a unary RPC, running in its own process.

It shares the same SQLite-backed job store (job_store.py) that app.py's
HTTP endpoint and background worker use -- this is the single source of
truth, not a wrapper around the HTTP endpoint. Logs go to the SAME
logs/app.log file app.py writes to, using the same job_id field, so
Kibana can correlate a job across both protocols.

Run this in its own terminal, alongside `python app.py`:

    python grpc_server.py
"""

import os
import logging
from concurrent import futures

import grpc
from pythonjsonlogger import jsonlogger

import job_service_pb2 as pb
import job_service_pb2_grpc as pb_grpc
import job_store

SERVICE_NAME = "invoice_job_queue_grpc"
GRPC_PORT = 50051

os.makedirs("logs", exist_ok=True)

logger = logging.getLogger("invoice_app_grpc")
logger.setLevel(logging.INFO)
log_handler = logging.FileHandler("logs/app.log")  # same log file as the HTTP app
formatter = jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
log_handler.setFormatter(formatter)
logger.addHandler(log_handler)


class JobServiceServicer(pb_grpc.JobServiceServicer):

    def SubmitJob(self, request, context):
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
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(validation_error)
            return pb.SubmitJobResponse()

        idempotency_key = request.idempotency_key or None
        job_id, status, is_replay = job_store.create_job(
            data["customer"], data["items"], idempotency_key
        )

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


def serve():
    job_store.init_db()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb_grpc.add_JobServiceServicer_to_server(JobServiceServicer(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    print(f"gRPC JobService listening on port {GRPC_PORT}...")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
