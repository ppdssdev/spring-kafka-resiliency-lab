#!/usr/bin/env python3
"""Deterministic demonstrations; only Python's standard library is needed."""
import json
import os
import sys
import time
import urllib.request
import uuid

BASE = os.environ.get("BASE_URL", "http://localhost:8088")

def request(path, body=None, method=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json", "X-Correlation-ID": "demo-" + str(uuid.uuid4())})
    with urllib.request.urlopen(req, timeout=15) as response:
        raw = response.read()
        return json.loads(raw) if raw else None

def wait_for(description, predicate, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.25)
    raise AssertionError("Timed out: " + description)

def create(failures):
    result = request("/demo/orders", {
        "order": {"customerReference": "portfolio-demo", "amount": 49.90, "currency": "USD"},
        "failures": failures})
    print("Created", json.dumps(result), flush=True)
    return result

def inspect(order):
    return request("/demo/orders/" + order["orderId"])

def processed(order):
    state = inspect(order)
    return state if state["status"] == "PROCESSED" else None

def normal():
    order = create(0)
    state = wait_for("normal processing", lambda: processed(order))
    assert state["attempts"] == 1 and state["processing_count"] == 1 and state["dedup_count"] == 1
    print("PASS normal processing:", json.dumps(state), flush=True)

def retry():
    order = create(2)
    # Observe a rolled-back failure before the scheduled recovery.
    state = wait_for("consumer failure", lambda: (s if (s := inspect(order))["attempts"] >= 1 else None))
    assert state["processing_count"] == 0 and state["dedup_count"] == 0
    print("PASS consumer failure rolled back:", json.dumps(state), flush=True)
    state = wait_for("retry and successful recovery", lambda: processed(order))
    assert state["attempts"] == 3 and state["processing_count"] == 1 and state["dead_letter_count"] == 0
    print("PASS retry and recovery:", json.dumps(state), flush=True)

def duplicate():
    order = create(0)
    wait_for("initial processing", lambda: processed(order))
    metrics_before = request_metrics()
    request("/demo/events/" + order["eventId"] + "/replay", method="POST")
    wait_for("duplicate observed by consumer", lambda: request_metrics() > metrics_before)
    state = inspect(order)
    assert state["processing_count"] == 1 and state["dedup_count"] == 1 and state["attempts"] == 1
    print("PASS duplicate ignored:", json.dumps(state), flush=True)

def request_metrics():
    with urllib.request.urlopen(BASE + "/actuator/prometheus", timeout=15) as response:
        for line in response.read().decode().splitlines():
            if line.startswith("lab_consumer_duplicates_total{"):
                return float(line.split()[-1])
    return 0

def dlq():
    order = create(-1)
    state = wait_for("exhausted retries and DLT audit",
        lambda: (s if (s := inspect(order))["dead_letter_count"] >= 1 else None))
    assert state["attempts"] == 4 and state["processing_count"] == 0 and state["dedup_count"] == 0
    letters = request("/demo/dead-letters")
    letter = next(item for item in letters if item["record_key"] == order["orderId"])
    assert json.loads(letter["payload"])["eventId"] == order["eventId"]
    assert letter["exception_message"] and letter["correlation_id"] == order["correlationId"]
    print("PASS exhausted retries and dead-letter topic:", json.dumps(state), flush=True)
    print("Dead-letter evidence:", json.dumps(letter), flush=True)
    # Operator-assisted recovery preserves the event ID and does not delete audit history.
    request("/demo/orders/" + order["orderId"] + "/failures", {"failures": 0}, "PUT")
    request("/demo/events/" + order["eventId"] + "/replay", method="POST")
    state = wait_for("recovery after repair and replay", lambda: processed(order))
    assert state["processing_count"] == 1 and state["dedup_count"] == 1
    print("PASS repaired and replayed:", json.dumps(state), flush=True)

if __name__ == "__main__":
    scenarios = {"normal": normal, "retry": retry, "duplicate": duplicate, "dlq": dlq}
    selected = sys.argv[1] if len(sys.argv) > 1 else "all"
    if selected == "all":
        for scenario in scenarios.values():
            scenario()
    elif selected in scenarios:
        scenarios[selected]()
    else:
        raise SystemExit("Usage: demo.py [all|normal|retry|duplicate|dlq]")
