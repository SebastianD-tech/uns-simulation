import paho.mqtt.client as mqtt
import time
import random
import json
import datetime
import sys
import os
import threading
from flask import Flask
import ssl

# =============================================================================
# TEIL 1: FLASK WEBSERVER FÜR HEALTH CHECKS
# =============================================================================
app = Flask(__name__)

@app.route('/')
def health_check():
    return "Sensor Simulator Service ist aktiv und läuft.", 200

def run_web_server():
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

# =============================================================================
# TEIL 2: MQTT KONFIGURATION & SIMULATIONS-LOGIK
# =============================================================================
MQTT_BROKER_HOST = os.environ.get("MQTT_BROKER_HOST")
MQTT_BROKER_PORT = int(os.environ.get("MQTT_BROKER_PORT", 8883))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")

if not all([MQTT_BROKER_HOST, MQTT_BROKER_PORT, MQTT_USERNAME, MQTT_PASSWORD]):
    print("FATALER FEHLER: Eine oder mehrere MQTT-Umgebungsvariablen fehlen.")
    sys.exit(1)

UNS_BASE_TOPIC = "LH/LBC/Biberach"

ASSETS = {
    "Fraesmaschine_01": {
        "bereich": "Produktion_A",
        "sensoren": ["Temperatur", "Druck", "Vibration", "Status", "Teilezaehler"]
    },
    "Verpackungslinie_07": {
        "bereich": "Logistik",
        "sensoren": ["Bandgeschwindigkeit", "Status", "Pakete_pro_Minute"]
    },
    "Lagerroboter_03": {
        "bereich": "Lagerhalle_B",
        "sensoren": ["Batteriestatus", "Position_X", "Position_Y", "Status"]
    }
}

class SensorSimulator:
    def __init__(self, asset_name):
        if asset_name not in ASSETS:
            print(f"Fehler: Anlage '{asset_name}' ist nicht in der ASSETS-Konfiguration definiert.")
            sys.exit(1)

        self.asset_name = asset_name
        self.config = ASSETS[asset_name]
        self.base_topic = f"{UNS_BASE_TOPIC}/{self.config['bereich']}/{self.asset_name}"
        self.state = {"teilezaehler": 0, "pakete_pro_minute": 0}

        self.client = mqtt.Client(client_id=f"{asset_name}_sim_{random.randint(1000,9999)}")
        self.client.on_connect = self.on_connect
        self.client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        self.client.tls_set(tls_version=ssl.PROTOCOL_TLS)
        self.client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"MQTT-SIMULATOR: Erfolgreich verbunden für '{self.asset_name}'")
        else:
            print(f"MQTT-SIMULATOR: Verbindung fehlgeschlagen mit Fehlercode {rc}")

    def get_sensor_value(self, sensor_name):
        if sensor_name == "Temperatur": return round(random.uniform(80.0, 95.0), 2), "°C"
        if sensor_name == "Druck": return round(random.uniform(5.0, 5.5), 2), "bar"
        if sensor_name == "Vibration": return round(random.uniform(0.1, 0.5) + random.choices([0, 1.5], weights=[0.95, 0.05])[0], 3), "mm/s"
        if sensor_name == "Status": return random.choices(["Running", "Idle", "Error"], weights=[0.9, 0.08, 0.02])[0], None
        if sensor_name == "Teilezaehler":
            self.state["teilezaehler"] += 1
            return self.state["teilezaehler"], "Stk"
        if sensor_name == "Bandgeschwindigkeit": return round(random.uniform(1.5, 1.8), 2), "m/s"
        if sensor_name == "Pakete_pro_Minute": return random.randint(18, 22), "pkg/min"
        if sensor_name == "Batteriestatus": return round(random.uniform(70.0, 99.9), 1), "%"
        if sensor_name == "Position_X": return round(random.uniform(10.0, 500.0), 2), "m"
        if sensor_name == "Position_Y": return round(random.uniform(10.0, 800.0), 2), "m"
        return None, None

    def run(self):
        try:
            while True:
                print(f"--- Sende Daten für {self.asset_name} ---")
                for sensor in self.config["sensoren"]:
                    value, unit = self.get_sensor_value(sensor)
                    if value is not None:
                        payload = {"value": value, "timestamp": datetime.datetime.now().isoformat()}
                        if unit:
                            payload["unit"] = unit
                        topic = f"{self.base_topic}/{sensor}"
                        self.client.publish(topic, json.dumps(payload), qos=1)
                        print(f"  > {topic}: {json.dumps(payload)}")
                time.sleep(random.randint(5, 10))
        except KeyboardInterrupt:
            print(f"\nSimulation für '{self.asset_name}' wird beendet.")
        finally:
            self.client.loop_stop()
            self.client.disconnect()

# =============================================================================
# TEIL 3: STARTPUNKT DES PROGRAMMS
# =============================================================================
if __name__ == "__main__":
    web_thread = threading.Thread(target=run_web_server)
    web_thread.daemon = True
    web_thread.start()
    print("HEALTH CHECK SERVER: Webserver wird im Hintergrund gestartet...")

    simulator_threads = []
    for asset_name in ASSETS.keys():
        print(f"Starte Simulation für: {asset_name}")
        simulator = SensorSimulator(asset_name)
        thread = threading.Thread(target=simulator.run)
        thread.daemon = True
        thread.start()
        simulator_threads.append(thread)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nSimulation wird beendet.")
