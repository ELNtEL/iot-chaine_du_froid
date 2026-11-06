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
HOST, PORT = "localhost", 1883

topic = f"tenant/{TENANT_ID}/depot/{DEPOT_ID}/sensor/{DEVICE_ID}"

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
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