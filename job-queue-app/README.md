# Invoice Job Queue

A small job queue system for a shop that needs to make customer invoices without making anyone wait. Built for Enterprise Software Development, Fall 2026.

- **Assignment 1:** observability. Metrics, dashboards and searchable logs.
- **Assignment 2:** a cleaner HTTP API, a gRPC service in its own process, and more metrics for both.

## Problem

Small shops need to make invoices for customer orders. Doing this by hand for each order wastes the owner's time and makes them stop other work. If many orders arrive at the same time, the owner has to handle them one by one instead of running the shop.

## Users

- **Shop owners (main user):** save time because they do not make each invoice by hand.
- **Customers (indirect user):** get their invoice without waiting for the owner to stop and make it.

## Solution

The shop owner sends order details to a simple API. The job is saved with a "pending" status. A background worker keeps checking for pending jobs and handles them in the order they arrived. For each one it makes a real PDF invoice with every item listed. The owner and the customer can check a job at any time. The status is `pending`, `in_progress`, `done` or `failed`.

## What Works

### Assignment 1

- Send a new invoice job through the API, with one or more items per order.
- Jobs are handled in the order they arrive (first come, first served).
- A background worker picks up pending jobs and makes the PDF invoice.
- You can check a job's status at any time. When it is done, the answer includes the path of the PDF.
- Application metrics (counters, a gauge, histograms and a summary) at `/metrics`, shown in Grafana.
- Machine metrics (CPU, memory, disk, network) collected two ways, so both views can be compared.
- Structured JSON logs sent through Filebeat into Elasticsearch, searchable in Kibana, with a retention policy.
- A `/chaos` endpoint that can add delays or random failures, to test if the monitoring really catches problems.

### Assignment 2

- **Better HTTP API:** clear status codes, one error format, an OpenAPI file, idempotency keys, rate limiting, and a Python caller with a timeout, retries and a circuit breaker.
- **gRPC service:** the same "submit a job" action as a gRPC call, in its own process, using the same business rules and the same database as the HTTP API.
- **More metrics:** HTTP and gRPC metrics side by side in one Grafana dashboard.

## Tech Stack

- **Backend:** Python, Flask, gRPC
- **Storage:** SQLite (shared by the HTTP and gRPC servers)
- **PDF:** fpdf2
- **Metrics:** prometheus_client, Prometheus, Grafana, Node Exporter, windows_exporter
- **Logging:** Python logging, python-json-logger, Filebeat, Elasticsearch, Kibana
- **Infrastructure:** Docker

## How It Is Built (Assignment 2)

Two server programs share one database, `jobs.db`:

```
 HTTP client  --->  app.py (Flask, port 5000)  --+
                    + background worker          +--->  job_store.py  --->  jobs.db (SQLite)
 gRPC client  --->  grpc_server.py (port 50051) -+
```

- `job_store.py` holds the shared rules: checking input, making jobs, idempotency and status changes. Both servers use it. A job sent over gRPC can be read over HTTP.
- Both programs write JSON logs to the same file, `logs/app.log`, with the same `job_id` field. This lets Kibana follow one job across both protocols.
- Each program has its own metrics page. `app.py` uses port 5000 at `/metrics`. `grpc_server.py` uses port 8001 at `/metrics`.

## Project Structure

```
job-queue-app/
├── app.py                      HTTP API, background worker, HTTP metrics, chaos endpoints
├── grpc_server.py              gRPC service (own process) and its metrics page
├── job_store.py                shared business rules and SQLite storage
├── job_service.proto           gRPC contract
├── job_service_pb2.py          generated from the .proto (do not edit)
├── job_service_pb2_grpc.py     generated from the .proto (do not edit)
├── grpc_client.py              Python gRPC client (one success, one failure)
├── caller.py                   HTTP caller with timeout, retry and circuit breaker
├── generate_traffic.py         repeatable success and failure traffic for the dashboards
├── openapi.yaml                OpenAPI 3.0.3 description of the HTTP API
├── test_openapi.py             checks live responses against openapi.yaml
├── test_pdf.py                 tests PDF making on its own
├── prometheus.yml              Prometheus settings (HTTP app, gRPC app, machine exporters)
├── grafana_dashboard_a2.json   Grafana dashboard (HTTP and gRPC side by side)
├── filebeat.yml                Filebeat settings (sends logs onward)
├── requirements.txt
├── invoices/                   generated PDFs (not committed)
└── logs/app.log                JSON logs (not committed)
```

## Setup

From the project folder, in PowerShell:

```
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run

Open two terminals. Activate the venv in each one and go to the project folder.

**Terminal 1: HTTP API and worker**
```
python app.py
```

**Terminal 2: gRPC service**
```
python grpc_server.py
```

Ports: HTTP `5000`, gRPC `50051`, gRPC metrics `8001`.

## Try It

### 1. Send a job (HTTP)

Send a POST request to `http://127.0.0.1:5000/jobs` with a JSON body like this:

```
{
  "customer": "Hira Naveed",
  "items": [
    {"name": "Chocolate Eclairs", "qty": 4, "price": 6},
    {"name": "Blueberry Macarons", "qty": 6, "price": 3},
    {"name": "Tiramisu Slice", "qty": 1, "price": 15},
    {"name": "Red Velvet Cupcake", "qty": 4, "price": 4}
  ]
}
```

You get back status `201` and:

```
{
  "job_id": "<id>",
  "status": "pending"
}
```

The `Location` header points to the new job. You can also add an `Idempotency-Key` header. If you send the same key again, you get the original job back with status `200` and an `Idempotent-Replayed: true` header. No second job is made.

### 2. Check the job

Send a GET request to `http://127.0.0.1:5000/jobs/<job_id>`. If you check at different times, you see the job move from `pending` to `in_progress` to `done`:

```
{
  "status": "done",
  "customer": "Hira Naveed",
  "items": [...],
  "file": "invoices/invoice_<job_id>.pdf"
}
```

### 3. Look at the invoice

Open the file named in `"file"`, inside the `invoices/` folder, to see the PDF.

### 4. Send a job with gRPC

```
python grpc_client.py
```

This makes one good call and one bad call. The bad call (empty items) returns `INVALID_ARGUMENT`. A job made through gRPC can be read with the same GET request from step 2.

### 5. View metrics

- Raw HTTP app metrics: `http://127.0.0.1:5000/metrics`
- Raw gRPC metrics: `http://127.0.0.1:8001/metrics`
- Dashboards: Grafana (see the Grafana section below).

### 6. Search logs

When Filebeat, Elasticsearch and Kibana are running, search the logs in Kibana Discover. For example: `event_type: "job_completed"`, or filter by one `job_id`.

### 7. Test fault injection

```
GET  http://127.0.0.1:5000/chaos
POST http://127.0.0.1:5000/chaos   body: {"slow_every_nth": 3}
POST http://127.0.0.1:5000/chaos   body: {"failure_rate": 0.3}
POST http://127.0.0.1:5000/chaos   body: {"fail_next_n_requests": 2}
POST http://127.0.0.1:5000/chaos   body: {"timeout_next_n_requests": 2}
```

The first two settings affect the background worker (slow or failed jobs). The last two affect the next requests to `POST /jobs` (an error response, or a 5 second delay).

## Tests and Demos

Run these in a third terminal while both servers are running.

| Command | What it shows |
|---|---|
| `python test_openapi.py` | Live HTTP answers match `openapi.yaml` (success and error cases) |
| `python caller.py` | Timeout, retry with backoff, idempotency key and circuit breaker, against injected faults |
| `python grpc_client.py` | One good and one failing (`INVALID_ARGUMENT`) gRPC call |
| `python generate_traffic.py 60` | A fixed mix of success and failure calls to HTTP and gRPC (60 rounds, about 1 minute). It fills the dashboards. |
| `python test_pdf.py` | PDF making on its own |

**Rate limiting:** send many quick requests to `POST /jobs` without the `X-Skip-Rate-Limit` header. The first 5 in 60 seconds work. After that you get `429` with a `Retry-After` header. The header `X-Skip-Rate-Limit: true` is only for testing. `caller.py` and `generate_traffic.py` use it so the limiter does not block those demos.

## Change the gRPC Code

You only need this if `job_service.proto` changes:

```
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. job_service.proto
```

## HTTP API Summary

| Method | Path | Success | Errors |
|---|---|---|---|
| POST | `/jobs` | `201` and `Location` (new job), or `200` and `Idempotent-Replayed: true` (repeat of a key) | `400`, `429`, `500` |
| GET | `/jobs/{job_id}` | `200` | `404` |
| GET and POST | `/chaos` | `200` | `400` |
| GET | `/metrics` | `200` | |

Every error uses the same shape: `{"error": {"code": "...", "message": "..."}}`. The full description is in `openapi.yaml`.

## gRPC API Summary

`JobService.SubmitJob(SubmitJobRequest) returns (SubmitJobResponse)`. See `job_service.proto`. Bad input returns the status `INVALID_ARGUMENT`. The client uses a 3 second deadline.

## Metrics

Prometheus reads three kinds of targets (set in `prometheus.yml`): the HTTP app (port 5000), the gRPC app (port 8001) and the machine exporters. After you change `prometheus.yml`, restart the Prometheus container with `docker restart <container name>`.

### From Assignment 1

| Metric | Type | Meaning |
|---|---|---|
| `jobs_submitted_total` | Counter | Jobs submitted (Assignment 2 added a `protocol` label) |
| `jobs_in_progress` | Gauge | Jobs that are pending or being worked on |
| `job_processing_seconds` | Histogram | Time to process a job |
| `invoice_file_size_kb` | Summary | Size of the PDFs |
| `invoice_total_value` | Histogram | Dollar value of invoices |
| `jobs_failed_total` | Counter | Jobs that failed |

`jobs_in_progress` is now read from the database. This keeps it correct whether the job came from HTTP or gRPC.

### New in Assignment 2

| Metric | Type | Labels | Where |
|---|---|---|---|
| `http_requests_total` | Counter | `method`, `endpoint`, `status` | `app.py` |
| `http_request_duration_seconds` | Histogram | `method`, `endpoint` | `app.py` |
| `rate_limit_rejections_total` | Counter | none | `app.py` |
| `idempotent_replays_total` | Counter | `protocol` | `app.py`, `grpc_server.py` |
| `jobs_submitted_total` | Counter | `protocol` (`http` or `grpc`) | `app.py`, `grpc_server.py` |
| `grpc_requests_total` | Counter | `method`, `code` | `grpc_server.py` |
| `grpc_request_duration_seconds` | Histogram | `method` | `grpc_server.py` |
| `grpc_requests_in_flight` | Gauge | `method` | `grpc_server.py` |

Durations are measured **on the server**, around the handler. They do not include network time. Labels always come from small fixed sets (method, route name, status code, protocol). User IDs, request IDs and trace IDs are never used as labels. The `/cardinality-test` endpoint from Assignment 1 shows what goes wrong when they are.

## Grafana Dashboard

`grafana_dashboard_a2.json` shows HTTP and gRPC panels side by side:

- calls per second by method or endpoint and status
- error rate
- p50 and p95 latency over a 5 minute window
- gRPC calls in flight
- jobs submitted, replays and rate-limit rejections

To load it, open Grafana, go to **Dashboards**, then **New**, then **Import**. Upload the JSON file and pick the Prometheus data source. Run `python generate_traffic.py 60` and set the time range to the last 15 minutes to see data.

## Logs

Structured JSON logs go to `logs/app.log`. Filebeat sends them to Elasticsearch and you can search them in Kibana, for example `event_type: "job_completed"` or by `job_id`. The `app_service` field tells the two programs apart: `invoice_job_queue` (HTTP) and `invoice_job_queue_grpc` (gRPC).

## Machine Metrics: Virtual Layer vs Real Host (Assignment 1)

CPU, memory, disk and network are each collected two ways:

- **Node Exporter**, running in Docker, reports on the WSL2 layer that Docker Desktop uses on Windows. Its CPU and memory numbers are close to the real machine, because WSL2 shares the real hardware. Its disk numbers are not close, because they show WSL2's own small virtual disk and not the real Windows drive.
- **windows_exporter**, running directly on Windows (not in Docker), reports on the real machine with no virtual layer. It is the right source for disk usage. It is also collected for CPU, memory and network so the two sources can be compared.

The total physical memory for the windows_exporter memory calculation was found once with `Get-CimInstance Win32_ComputerSystem` in PowerShell. It is used as a fixed number in the Grafana query, because this windows_exporter build does not give total memory as its own metric with the default settings.

## Known Limitations

- In Assignment 1, all data lived in memory and was lost on restart, as the assignment asked (no database system). Assignment 2 moved jobs into SQLite so that two programs can share them.
- `X-Skip-Rate-Limit` is for testing only. It would be removed in a real system.
- The rate limiter keeps its counts in memory, so they reset when the app restarts.
- Grafana dashboards live inside the Grafana container. If the container is removed, import `grafana_dashboard_a2.json` again. The Assignment 1 panels must be rebuilt from the queries in the Assignment 1 report.
- `grpc_requests_in_flight` stays close to 0 under light load, because calls finish in a few milliseconds.
