"""
Python client for the gRPC JobService. Calls SubmitJob directly, with a
deadline (timeout), and reports the result or the gRPC error.

Run this while grpc_server.py is running:

    python grpc_client.py
"""

import uuid
import grpc

import job_service_pb2 as pb
import job_service_pb2_grpc as pb_grpc

GRPC_TARGET = "127.0.0.1:50051"
DEADLINE_SECONDS = 3


def submit_job(customer, items, idempotency_key=None):
    channel = grpc.insecure_channel(GRPC_TARGET)
    stub = pb_grpc.JobServiceStub(channel)

    request = pb.SubmitJobRequest(
        customer=customer,
        items=[pb.JobItem(name=i["name"], qty=i["qty"], price=i["price"]) for i in items],
        idempotency_key=idempotency_key or ""
    )

    try:
        response = stub.SubmitJob(request, timeout=DEADLINE_SECONDS)
        print(f"SUCCESS -> job_id={response.job_id}, status={response.status}, "
              f"idempotent_replayed={response.idempotent_replayed}")
        return response
    except grpc.RpcError as e:
        print(f"FAILED -> gRPC status: {e.code()} ({e.code().name}), details: {e.details()}")
        return None


if __name__ == "__main__":
    print("=" * 60)
    print("TEST 1: valid job submission -> expect OK")
    print("=" * 60)
    submit_job(
        "gRPC Test Customer",
        [{"name": "Chocolate Eclairs", "qty": 2, "price": 6}],
        idempotency_key=str(uuid.uuid4())
    )

    print()
    print("=" * 60)
    print("TEST 2: invalid job submission (empty items) -> expect INVALID_ARGUMENT")
    print("=" * 60)
    submit_job("gRPC Test Customer", [])
