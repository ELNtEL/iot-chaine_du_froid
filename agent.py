"""Step 13: step 12, plus the agent logs in to Mosquitto with a username and password."""
import asyncio
import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Optional

import httpx
import paho.mqtt.client as mqtt
from fastapi import Body, FastAPI, Header
from fastapi.responses import JSONResponse

DB_FILE = "agent.db"
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "")           # empty = connect without a login (only works if Mosquitto allows anonymous)
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
NOYAU_URL = os.getenv("NOYAU_URL", "http://localhost:8000")
STARTED_AT = time.time()
DEFAULT_LIMITS = {"temp_min": 2.0, "temp_max": 8.0, "offline_timeout_sec": 120}
WATCHDOG_INTERVAL = int(os.getenv("WATCHDOG_INTERVAL_SEC", "30"))   # how often we look for silent sensors
RETRY_WINDOW = 3600     # keep retrying an undelivered alert for up to 1 hour
EXCHANGE_INTERVAL = int(os.getenv("EXCHANGE_PUBLISH_INTERVAL_SEC", "300"))   # publish at least this often
last_published = {}     # (tenant, depot) -> when we last published, kept in memory

# InfluxDB (history). It stays switched OFF until INFLUX_TOKEN is set, so the agent also works without it.
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086").rstrip("/")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")
INFLUX_ORG = os.getenv("INFLUX_ORG", "estin_iot")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "cold_chain_telemetry")
influx = httpx.Client(timeout=2.0)       # short timeout: a slow InfluxDB must not freeze the MQTT thread
_last_influx_warning = 0


# ---------------------------------------------------------------- database
def db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db()
    conn.execute("""CREATE TABLE IF NOT EXISTS tenants (
                        tenant_id TEXT PRIMARY KEY, api_key TEXT NOT NULL, config_json TEXT NOT NULL)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS last_reading (
                        tenant_id TEXT NOT NULL, depot_id TEXT NOT NULL,
                        temperature REAL, status TEXT, ts INTEGER,
                        PRIMARY KEY (tenant_id, depot_id))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS alert_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT, tenant_id TEXT NOT NULL,
                        event_type TEXT NOT NULL, severity TEXT NOT NULL, data_json TEXT NOT NULL,
                        created_at INTEGER NOT NULL, delivered INTEGER NOT NULL DEFAULT 0,
                        attempts INTEGER NOT NULL DEFAULT 0)""")
    # delivered: 0 = waiting, 1 = the Noyau confirmed it, 2 = the Noyau refused it (retrying is useless)
    conn.commit()
    conn.close()


init_db()


# ---------------------------------------------------------------- alerts to the Noyau
def send_event(tenant, api_key, event_type, severity, data):
    """Save the alert in the notebook FIRST, then try to deliver it."""
    conn = db()
    cur = conn.execute("INSERT INTO alert_log (tenant_id, event_type, severity, data_json, created_at) "
                       "VALUES (?, ?, ?, ?, ?)", (tenant, event_type, severity, json.dumps(data), int(time.time())))
    alert_id = cur.lastrowid
    conn.commit()
    conn.close()
    deliver(alert_id, tenant, api_key, event_type, severity, data)


def deliver(alert_id, tenant, api_key, event_type, severity, data):
    """One attempt to POST an alert to the Noyau. Never crashes the agent."""
    outcome = 0                                   # 0 = failed, will retry
    try:
        resp = httpx.post(
            f"{NOYAU_URL}/webhooks/agent-events",
            headers={"Authorization": f"Bearer {api_key}", "X-Tenant-Id": tenant},
            json={"event_type": event_type, "severity": severity, "data": data},
            timeout=5.0,
        )
        print(f"NOYAU: sent {event_type}/{severity} for {tenant} -> HTTP {resp.status_code}")
        if 200 <= resp.status_code < 300:
            outcome = 1                           # delivered
        elif 400 <= resp.status_code < 500:
            outcome = 2                           # the Noyau said no: retrying will not help
    except httpx.HTTPError as exc:
        print(f"NOYAU: could not send {event_type} for {tenant} ({type(exc).__name__}), will retry")

    conn = db()
    conn.execute("UPDATE alert_log SET attempts = attempts + 1, delivered = ? WHERE id = ?", (outcome, alert_id))
    conn.commit()
    conn.close()


def retry_undelivered():
    """Send again every alert that is still waiting (oldest first, within the last hour)."""
    conn = db()
    rows = conn.execute(
        "SELECT a.id, a.tenant_id, a.event_type, a.severity, a.data_json, t.api_key "
        "FROM alert_log a JOIN tenants t ON t.tenant_id = a.tenant_id "
        "WHERE a.delivered = 0 AND a.created_at >= ? ORDER BY a.id LIMIT 50",
        (int(time.time()) - RETRY_WINDOW,)).fetchall()
    conn.close()
    for r in rows:
        data = json.loads(r["data_json"])
        data["renvoi"] = True                     # tells the Noyau this is a re-send
        print(f"RETRY: alert #{r['id']} ({r['event_type']}) for {r['tenant_id']}")
        deliver(r["id"], r["tenant_id"], r["api_key"], r["event_type"], r["severity"], data)


# ---------------------------------------------------------------- InfluxDB history
def influx_warn(message):
    """Print at most one InfluxDB warning per minute, so a dead database does not flood the screen."""
    global _last_influx_warning
    if time.time() - _last_influx_warning > 60:
        _last_influx_warning = time.time()
        print("INFLUX:", message)


def influx_tag(value):
    """Escape a tag value for InfluxDB line protocol."""
    return str(value).replace("\\", "\\\\").replace(",", "\\,").replace("=", "\\=").replace(" ", "\\ ")


def influx_write(tenant, depot, device, data, ts):
    """Store one reading. The tenant is a TAG on every point, so one company's data can be selected or deleted alone."""
    if not INFLUX_TOKEN:
        return
    fields = []
    for name in ("temperature_celsius", "probe_temperature_celsius", "humidity_percent", "battery_voltage"):
        value = data.get(name)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            fields.append(f"{name}={float(value)}")
    if isinstance(data.get("door_open"), bool):
        fields.append(f"door_open={'true' if data['door_open'] else 'false'}")
    if not fields:
        return
    line = (f"cold_chain_reading,tenant_id={influx_tag(tenant)},depot_id={influx_tag(depot)},"
            f"device_id={influx_tag(device)} {','.join(fields)} {ts}")
    try:
        resp = influx.post(f"{INFLUX_URL}/api/v2/write",
                           params={"org": INFLUX_ORG, "bucket": INFLUX_BUCKET, "precision": "s"},
                           headers={"Authorization": f"Token {INFLUX_TOKEN}"}, content=line)
        if resp.status_code != 204:
            influx_warn(f"write refused: HTTP {resp.status_code} {resp.text[:120]}")
    except httpx.HTTPError as exc:
        influx_warn(f"could not write ({type(exc).__name__}): this reading is not in the history")


def influx_delete(tenant):
    """Delete ALL the history of one company. Returns True if done (or if InfluxDB is not used)."""
    if not INFLUX_TOKEN:
        return True
    if '"' in tenant:
        return False                                  # never build a delete rule from a suspicious name
    stop = datetime.fromtimestamp(time.time() + 86400, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        resp = influx.post(f"{INFLUX_URL}/api/v2/delete",
                           params={"org": INFLUX_ORG, "bucket": INFLUX_BUCKET},
                           headers={"Authorization": f"Token {INFLUX_TOKEN}"},
                           json={"start": "1970-01-01T00:00:00Z", "stop": stop,
                                 "predicate": f'_measurement="cold_chain_reading" AND tenant_id="{tenant}"'})
        return resp.status_code == 204
    except httpx.HTTPError:
        return False


# ---------------------------------------------------------------- shared exchange
def publish_measure(tenant, api_key, depot, device, temp, status, data, limits, now_iso):
    """Publish the current state of a sensor as a 'mesure_capteur' object on the Noyau exchange."""
    last_published[(tenant, depot)] = time.time()    # throttle: set before trying, even if it fails
    payload = {
        # fields required by the shared registry (INTER_AGENT_DATA_CONTRACT, section 8)
        "capteur_id": device,
        "type_mesure": "temperature",
        "valeur": temp,
        "unite": "celsius",
        "horodatage": now_iso,
        # extra fields: other agents ignore the ones they do not know
        "depot_id": depot,
        "statut": status,
        "temperature_sonde_celsius": data.get("probe_temperature_celsius"),
        "humidite_pourcent": data.get("humidity_percent"),
        "seuil_min": limits["temp_min"],
        "seuil_max": limits["temp_max"],
    }
    try:
        resp = httpx.post(
            f"{NOYAU_URL}/exchange/agent/mesure_capteur",
            headers={"Authorization": f"Bearer {api_key}", "X-Tenant-Id": tenant},
            json={"external_id": device, "payload": payload},     # same external_id = the object is updated
            timeout=5.0,
        )
        print(f"EXCHANGE: published mesure_capteur {device} ({status}) -> HTTP {resp.status_code}")
    except httpx.HTTPError as exc:
        print(f"EXCHANGE: could not publish {device} ({type(exc).__name__})")


# ---------------------------------------------------------------- silent sensors
def check_silent_sensors():
    """Look at every depot: if its last reading is too old, mark it OFFLINE and tell the Noyau (once)."""
    now = int(time.time())
    conn = db()
    rows = conn.execute(
        "SELECT l.tenant_id, l.depot_id, l.ts, t.api_key, t.config_json "
        "FROM last_reading l JOIN tenants t ON t.tenant_id = l.tenant_id "
        "WHERE l.status != 'OFFLINE'").fetchall()

    silent = []
    for r in rows:
        limits = {**DEFAULT_LIMITS, **json.loads(r["config_json"])}
        silence = now - r["ts"]
        if silence > limits["offline_timeout_sec"]:
            conn.execute("UPDATE last_reading SET status = 'OFFLINE' WHERE tenant_id = ? AND depot_id = ?",
                         (r["tenant_id"], r["depot_id"]))
            silent.append((r, silence, limits["offline_timeout_sec"]))
    conn.commit()
    conn.close()

    for r, silence, timeout in silent:      # send AFTER closing the database
        last_seen = datetime.fromtimestamp(r["ts"], timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"WATCHDOG: no reading from {r['tenant_id']}/{r['depot_id']} for {silence}s")
        send_event(r["tenant_id"], r["api_key"], "capteur_deconnecte", "critical", {
            "depot_id": r["depot_id"], "silence_sec": silence, "derniere_trame": last_seen,
            "message": f"[{r['depot_id']}] no reading for {silence}s (limit {timeout}s)"})


async def watchdog_loop():
    while True:
        await asyncio.sleep(WATCHDOG_INTERVAL)
        try:
            await asyncio.to_thread(check_silent_sensors)   # run the blocking code off the main thread
            await asyncio.to_thread(retry_undelivered)
        except Exception as exc:
            print("WATCHDOG error:", exc)


# ---------------------------------------------------------------- MQTT side
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        # Mosquitto answered but said no (wrong or missing login). Say so clearly instead of pretending.
        print(f"MQTT: login REFUSED by Mosquitto ({reason_code}). Check MQTT_USER and MQTT_PASSWORD.")
        return
    print("MQTT: connected to Mosquitto" + (f" as '{MQTT_USER}'" if MQTT_USER else " (no login)"))
    client.subscribe("tenant/+/depot/+/sensor/+", qos=1)


def on_message(client, userdata, msg):
    # 1. read the letter
    parts = msg.topic.split("/")
    try:
        data = json.loads(msg.payload)
        tenant, depot, device = data["tenant_id"], data["depot_id"], data["device_id"]
        temp = data["temperature_celsius"]
    except (ValueError, KeyError, TypeError):
        print("MQTT: ignored a badly formed message on", msg.topic)
        return
    if len(parts) != 6 or (parts[1], parts[3], parts[5]) != (tenant, depot, device):
        print("MQTT: REJECTED, topic and payload disagree:", msg.topic)
        return
    if not isinstance(temp, (int, float)):
        print("MQTT: ignored, no temperature in the message")
        return

    # 2. is this company in the notebook?
    conn = db()
    row = conn.execute("SELECT api_key, config_json FROM tenants WHERE tenant_id = ?", (tenant,)).fetchone()
    if row is None:
        conn.close()
        print(f"MQTT: IGNORED, company {tenant} is not active")
        return

    # what was the state of this depot BEFORE this reading?
    prev = conn.execute("SELECT status FROM last_reading WHERE tenant_id = ? AND depot_id = ?",
                        (tenant, depot)).fetchone()
    prev_status = prev["status"] if prev else "OK"

    # 3. use THIS company's limits (or the defaults)
    limits = {**DEFAULT_LIMITS, **json.loads(row["config_json"])}
    event_type, over, limit = None, 0, None
    if temp > limits["temp_max"]:
        status, event_type, limit, over = "ALERTE", "seuil_temperature_haut", limits["temp_max"], temp - limits["temp_max"]
        print(f"ALERT too warm: {temp} C in {tenant}/{depot} (max {limit})")
    elif temp < limits["temp_min"]:
        status, event_type, limit, over = "ALERTE", "seuil_temperature_bas", limits["temp_min"], limits["temp_min"] - temp
        print(f"ALERT too cold: {temp} C in {tenant}/{depot} (min {limit})")
    else:
        status = "OK"
        print(f"ok: {temp} C in {tenant}/{depot}")

    # 4. remember the latest reading, for /status
    conn.execute("INSERT OR REPLACE INTO last_reading (tenant_id, depot_id, temperature, status, ts) "
                 "VALUES (?, ?, ?, ?, ?)", (tenant, depot, temp, status, int(time.time())))
    conn.commit()
    conn.close()

    # 4b. keep the full history in InfluxDB (use the sensor's own clock when it is believable)
    sensor_ts = data.get("timestamp")
    now_s = int(time.time())
    if not isinstance(sensor_ts, int) or abs(now_s - sensor_ts) > 7 * 86400:
        sensor_ts = now_s
    influx_write(tenant, depot, device, data, sensor_ts)

    # 5. tell the Noyau, but only when the situation CHANGES (no message every 3 seconds)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    details = {"depot_id": depot, "device_id": device, "valeur": temp, "horodatage": now_iso}
    if prev_status == "OFFLINE":
        send_event(tenant, row["api_key"], "capteur_reconnecte", "info",
                   {"depot_id": depot, "device_id": device, "horodatage": now_iso,
                    "message": f"[{depot}] sensor {device} is back online"})
    if status == "ALERTE" and prev_status != "ALERTE":
        severity = "critical" if over > 3 else "high"
        details["seuil"] = limit
        details["message"] = f"[{depot}] temperature {temp} C, limit {limit} C"
        send_event(tenant, row["api_key"], event_type, severity, details)
    elif status == "OK" and prev_status == "ALERTE":
        details["message"] = f"[{depot}] back within limits ({temp} C)"
        send_event(tenant, row["api_key"], "retour_normal", "info", details)

    # 6. share the state on the exchange: when it changes, and at least every EXCHANGE_INTERVAL seconds
    due = time.time() - last_published.get((tenant, depot), 0) >= EXCHANGE_INTERVAL
    if status != prev_status or due:
        publish_measure(tenant, row["api_key"], depot, device, temp, status, data, limits, now_iso)


@asynccontextmanager
async def lifespan(app):
    # runs once when the server STARTS ...
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.connect_async(MQTT_HOST, MQTT_PORT)   # does not crash if Mosquitto is down
    client.loop_start()                          # listens in a background thread
    watchdog = asyncio.create_task(watchdog_loop())
    yield
    # ... and once when it STOPS
    watchdog.cancel()
    client.loop_stop()
    client.disconnect()


app = FastAPI(lifespan=lifespan)


# ---------------------------------------------------------------- HTTP side
def ok(data):
    return {"status": "ok", "data": data, "error": None}


def error(message, code):
    return JSONResponse(status_code=code,
                        content={"status": "error", "data": None, "error": message})


def get_key(authorization):
    if authorization and authorization.startswith("Bearer "):
        return authorization[7:]
    return None


@app.get("/health")
def health():
    return ok({"uptime_seconds": int(time.time() - STARTED_AT)})


@app.get("/config-schema")
def config_schema():
    return ok({
        "type": "object",
        "properties": {
            "temp_min": {"type": "number", "default": 2.0, "description": "Minimum allowed temperature (C)"},
            "temp_max": {"type": "number", "default": 8.0, "description": "Maximum allowed temperature (C)"},
            "humidity_max": {"type": "number", "default": 80.0, "description": "Maximum allowed humidity (%)"},
            "offline_timeout_sec": {"type": "integer", "default": 120,
                                    "description": "Seconds without a reading before a sensor is reported disconnected"},
        },
    })


@app.post("/tenant/activate")
def activate(body: Optional[dict] = Body(default=None),
             x_tenant_id: Optional[str] = Header(default=None),
             authorization: Optional[str] = Header(default=None)):
    key = get_key(authorization)
    if not x_tenant_id:
        return error("Missing X-Tenant-Id header", 400)
    if not key:
        return error("Missing Authorization: Bearer <key> header", 401)
    config = (body or {}).get("config", {})
    conn = db()
    conn.execute("INSERT OR REPLACE INTO tenants (tenant_id, api_key, config_json) VALUES (?, ?, ?)",
                 (x_tenant_id, key, json.dumps(config)))
    conn.commit()
    conn.close()
    return ok({"activated": True, "tenant_id": x_tenant_id})


@app.post("/tenant/deactivate")
def deactivate(x_tenant_id: Optional[str] = Header(default=None),
               authorization: Optional[str] = Header(default=None)):
    if not x_tenant_id:
        return error("Missing X-Tenant-Id header", 400)
    conn = db()
    row = conn.execute("SELECT api_key FROM tenants WHERE tenant_id = ?", (x_tenant_id,)).fetchone()
    if row is not None:
        if get_key(authorization) != row["api_key"]:
            conn.close()
            return error("Invalid key for this tenant", 401)
        for key in [k for k in last_published if k[0] == x_tenant_id]:
            last_published.pop(key)
        conn.execute("DELETE FROM alert_log WHERE tenant_id = ?", (x_tenant_id,))
        conn.execute("DELETE FROM last_reading WHERE tenant_id = ?", (x_tenant_id,))
        conn.execute("DELETE FROM tenants WHERE tenant_id = ?", (x_tenant_id,))
        conn.commit()
        conn.close()
        purged = influx_delete(x_tenant_id)       # False if InfluxDB could not be cleaned
        if not purged:
            print(f"INFLUX: WARNING, history of {x_tenant_id} could NOT be deleted")
        return ok({"deactivated": True, "data_purged": purged, "tenant_id": x_tenant_id})
    conn.close()
    return ok({"deactivated": True, "data_purged": True, "tenant_id": x_tenant_id})


@app.get("/tenant/{tenant_id}/status")
def status(tenant_id: str, authorization: Optional[str] = Header(default=None)):
    conn = db()
    row = conn.execute("SELECT * FROM tenants WHERE tenant_id = ?", (tenant_id,)).fetchone()
    if row is None:
        conn.close()
        return error("Unknown or inactive tenant", 404)
    if get_key(authorization) != row["api_key"]:
        conn.close()
        return error("Invalid key for this tenant", 401)
    depots = [dict(r) for r in conn.execute(
        "SELECT depot_id, temperature, status, ts FROM last_reading WHERE tenant_id = ? ORDER BY depot_id",
        (tenant_id,))]
    pending = conn.execute("SELECT COUNT(*) FROM alert_log WHERE tenant_id = ? AND delivered = 0",
                           (tenant_id,)).fetchone()[0]
    conn.close()
    return ok({"state": "running", "tenant_id": tenant_id, "pending_alerts": pending,
               "config": json.loads(row["config_json"]), "depots": depots})