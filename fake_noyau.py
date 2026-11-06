"""STAND-IN for the teacher's Noyau. It only receives alerts and prints them.
It checks the rules written in AGENT_INTEGRATION_CONTRACT section 5. It is NOT the real Noyau."""
import time
from typing import Optional
from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse

app = FastAPI()
received = []
SEVERITIES = {"info", "low", "medium", "high", "critical"}


def refuse(msg, code):
    return JSONResponse(status_code=code, content={"status": "error", "data": None, "error": msg})


@app.post("/webhooks/agent-events")
def agent_events(body: dict, x_tenant_id: Optional[str] = Header(default=None),
                 authorization: Optional[str] = Header(default=None)):
    if not x_tenant_id or not authorization or not authorization.startswith("Bearer "):
        return refuse("Missing X-Tenant-Id or Authorization: Bearer", 401)
    if not isinstance(body.get("event_type"), str) or body.get("severity") not in SEVERITIES \
            or not isinstance(body.get("data"), dict):
        return refuse("Body must be {event_type: str, severity: info|low|medium|high|critical, data: object}", 422)
    received.append({"tenant": x_tenant_id, "key_seen": authorization[7:], **body})
    print(f"NOYAU RECEIVED: {x_tenant_id} | {body['event_type']} | {body['severity']} | {body['data'].get('message')}", flush=True)
    return {"status": "ok", "data": {"event_id": len(received)}, "error": None}


@app.get("/events")
def events():
    return received