"""Step 2: listener with a threshold check."""
import json
import paho.mqtt.client as mqtt

# Allowed range for each depot, in degrees Celsius.
THRESHOLDS = {
    "entrepot_principal":  {"min": 2.0,   "max": 8.0},
    "chambre_congelation": {"min": -25.0, "max": -18.0},
}
DEFAULT = {"min": 2.0, "max": 8.0}      # used for a depot we don't know

def on_connect(client, userdata, flags, reason_code, properties):
    print("Connected to Mosquitto")
    client.subscribe("tenant/+/depot/+/sensor/+", qos=1)

def on_message(client, userdata, msg):
    parts = msg.topic.split("/")
    data = json.loads(msg.payload)
    if (parts[1], parts[3], parts[5]) != (data["tenant_id"], data["depot_id"], data["device_id"]):
        print("REJECTED: topic and payload disagree", msg.topic)
        return

    temp = data["temperature_celsius"]
    limits = THRESHOLDS.get(data["depot_id"], DEFAULT)

    if temp > limits["max"]:
        print(f"ALERT too warm: {temp} C in {data['depot_id']} (max {limits['max']})")
    elif temp < limits["min"]:
        print(f"ALERT too cold: {temp} C in {data['depot_id']} (min {limits['min']})")
    else:
        print(f"ok: {temp} C in {data['depot_id']} (allowed {limits['min']} to {limits['max']})")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message
client.connect("localhost", 1883)
client.loop_forever()