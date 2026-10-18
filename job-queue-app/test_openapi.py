"""
Uses openapi.yaml as the source of truth to test real responses from the
running Flask app. This is the "use it to inspect or test at least one
endpoint" requirement for Assignment 2 Part A.

Run this while app.py is running (python app.py in another terminal).

    pip install pyyaml jsonschema requests
    python test_openapi.py
"""

import sys
import uuid
import yaml
import requests
from jsonschema import validate, ValidationError

BASE_URL = "http://127.0.0.1:5000"
SPEC_FILE = "openapi.yaml"


def load_spec():
    with open(SPEC_FILE, "r") as f:
        return yaml.safe_load(f)


def resolve_ref(spec, ref):
    """Resolves a '#/components/schemas/Xyz' style $ref into the actual schema dict."""
    assert ref.startswith("#/")
    parts = ref.lstrip("#/").split("/")
    node = spec
    for part in parts:
        node = node[part]
    return node


def get_response_schema(spec, path, method, status_code):
    """Pulls the JSON schema for a given path/method/status straight out of the spec."""
    operation = spec["paths"][path][method]
    response = operation["responses"][str(status_code)]
    schema = response["content"]["application/json"]["schema"]
    if "$ref" in schema:
        schema = resolve_ref(spec, schema["$ref"])
    return schema


def check(label, condition):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {label}")
    return condition


def main():
    spec = load_spec()
    all_passed = True

    print("=" * 60)
    print("TEST 1: POST /jobs with a valid body -> expect 201,")
    print("        body matching JobCreatedResponse schema in openapi.yaml")
    print("=" * 60)

    valid_body = {
        "customer": "OpenAPI Test Customer",
        "items": [{"name": "Macaron", "qty": 2, "price": 3}]
    }
    idem_key = str(uuid.uuid4())
    resp = requests.post(
        f"{BASE_URL}/jobs",
        json=valid_body,
        headers={"Idempotency-Key": idem_key}
    )

    all_passed &= check("Status code is 201", resp.status_code == 201)
    all_passed &= check("'Location' header present", "Location" in resp.headers)

    schema = get_response_schema(spec, "/jobs", "post", 201)
    try:
        validate(instance=resp.json(), schema=schema)
        all_passed &= check("Response body matches JobCreatedResponse schema", True)
    except ValidationError as e:
        all_passed &= check(f"Response body matches JobCreatedResponse schema ({e.message})", False)

    print()
    print("=" * 60)
    print("TEST 2: Repeat the exact same request (same Idempotency-Key)")
    print("        -> expect 200, same job_id, matching schema")
    print("=" * 60)

    first_job_id = resp.json()["job_id"]
    resp2 = requests.post(
        f"{BASE_URL}/jobs",
        json=valid_body,
        headers={"Idempotency-Key": idem_key}
    )

    all_passed &= check("Status code is 200 (idempotent replay)", resp2.status_code == 200)
    all_passed &= check("job_id matches the original request", resp2.json().get("job_id") == first_job_id)
    all_passed &= check("'Idempotent-Replayed' header is 'true'", resp2.headers.get("Idempotent-Replayed") == "true")

    print()
    print("=" * 60)
    print("TEST 3: POST /jobs with missing 'items' -> expect 400,")
    print("        body matching ErrorResponse schema in openapi.yaml")
    print("=" * 60)

    invalid_body = {"customer": "Missing Items Customer"}
    resp3 = requests.post(f"{BASE_URL}/jobs", json=invalid_body)

    all_passed &= check("Status code is 400", resp3.status_code == 400)

    error_schema = get_response_schema(spec, "/jobs", "post", 400)
    try:
        validate(instance=resp3.json(), schema=error_schema)
        all_passed &= check("Response body matches ErrorResponse schema", True)
    except ValidationError as e:
        all_passed &= check(f"Response body matches ErrorResponse schema ({e.message})", False)

    print()
    print("=" * 60)
    if all_passed:
        print("ALL CHECKS PASSED — live API responses match openapi.yaml")
    else:
        print("SOME CHECKS FAILED — see above")
    print("=" * 60)

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
