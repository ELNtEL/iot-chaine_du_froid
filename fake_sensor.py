"""Fake ESP32: publishes EXACTLY the topic and JSON the real firmware will publish."""
import json, os, random, sys, time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

TENANT_ID = os.environ.get("TENANT_ID", "comp_01")
DEPOT_ID = "entrepot_principal"
DEVICE_ID = "fake_01"
ZONE_TYPE = "chambre_froide_positive"
PERIOD_SEC = 3          # real firmware: 30
# python fake_sensor.py hot   -> temperatures between 9 and 12 (too warm)
# python fake_sensor.py       -> temperatures between 3 and 6 (normal)
HOT = len(sys.argv) > 1 and sys.argv[1] == "hot"
HOST = os.environ.get("MQTT_HOST", "localhost")
PORT = int(os.environ.get("MQTT_PORT", "1883"))
# A company's device logs in with the company's tenant id as its username (the broker's access rules expect that),
# and the password the company was given. Empty password = connect without a login.
DEVICE_PASSWORD = os.environ.get("DEVICE_PASSWORD", "")

topic = f"tenant/{TENANT_ID}/depot/{DEPOT_ID}/sensor/{DEVICE_ID}"

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f"LOGIN REFUSED by Mosquitto ({reason_code}). Check DEVICE_PASSWORD and TENANT_ID.")
    else:
        print("connected to Mosquitto" + (f" as '{TENANT_ID}'" if DEVICE_PASSWORD else " (no login)"))


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
if DEVICE_PASSWORD:
    client.username_pw_set(TENANT_ID, DEVICE_PASSWORD)
client.connect(HOST, PORT)
client.loop_start()

while True:
    now = time.time()
    air = round(random.uniform(9, 12) if HOT else random.uniform(3, 6), 2)
    reading = {
        "device_id": DEVICE_ID,
        "tenant_id": TENANT_ID,
        "depot_id": DEPOT_ID,
        "zone_type": ZONE_TYPE,
        "sensor_model": "SIM-DHT22+DS18B20",
        "timestamp": int(now),
        "iso_timestamp": datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "temperature_celsius": air,
        "probe_temperature_celsius": round(air - 0.3, 2),
        "humidity_percent": round(random.uniform(60, 70), 1),
        "door_open": False,
        "battery_voltage": 3.3,
        "status": "OK",
    }
    client.publish(topic, json.dumps(reading), qos=1)
    print("sent ->", topic)
    time.sleep(PERIOD_SEC)