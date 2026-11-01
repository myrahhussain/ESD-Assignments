import requests

URL = "http://127.0.0.1:5000/jobs"
BODY = {"customer": "Rate Limit Demo", "items": []}

print("Sending 7 quick requests. The limit is 5 per 60 seconds per client.\n")

for n in range(1, 8):
    r = requests.post(URL, json=BODY, timeout=5)
    retry_after = r.headers.get("Retry-After")
    message = r.json()["error"]["message"]

    line = f"request {n}: HTTP {r.status_code}"
    if retry_after:
        line += f"   Retry-After: {retry_after} seconds"
    print(line)
    print(f"           {message}")

print("\nDone. The limiter counts requests per client, and a request is allowed")
print("again once the oldest one leaves the 60 second window.")
