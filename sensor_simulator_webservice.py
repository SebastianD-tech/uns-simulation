import paho.mqtt.client as mqtt
import time
import random
import json
import datetime
import sys
import os
import threading  # HINZUGEFÜGT: Um Simulation und Webserver parallel zu starten
from flask import Flask # HINZUGEFÜGT: Für den Mini-Webserver

# --- Webserver für den Render Health Check ---
# HINZUGEFÜGT: Dieser ganze Block ist neu.
app = Flask(__name__)

@app.route('/')
def health_check():
    """Diese Funktion antwortet auf die Health Checks von Render."""
    return "Sensor Simulator ist aktiv und läuft.", 200

def run_web_server():
    """Startet den Flask Webserver."""
    # Render stellt den Port über eine Umgebungsvariable bereit.
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

# --- MQTT Konfiguration (unverändert) ---
MQTT_BROKER_HOST = os.environ.get("MQTT_BROKER_HOST")
MQTT_BROKER_PORT = int(os.environ.get("MQTT_BROKER_PORT", 1883))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")

if not all([MQTT_BROKER_HOST, MQTT_BROKER_PORT, MQTT_USERNAME, MQTT_PASSWORD]):
    print("Fehler: Umgebungsvariablen nicht vollständig gesetzt.")
    sys.exit(1)

UNS_BASE_TOPIC = "MyCompany/Biberach"
ASSETS = {
    "Fraesmaschine_01": {
        "bereich": "Produktion_A",
        "sensoren": ["Temperatur", "Druck", "Vibration", "Status", "Teilezaehler"]
    },
    # ... weitere Anlagen wie gehabt
}

# --- Die SensorSimulator-Klasse (unverändert) ---
class SensorSimulator:
    # Der Inhalt dieser Klasse ist exakt derselbe wie in der vorherigen Version.
    # Hier wird nichts geändert.
    def __init__(self, asset_name):
        # ... (kompletter Code der Klasse von vorher)
        if asset_name not in ASSETS:
            print(f"Fehler: Anlage '{asset_name}' nicht in ASSETS gefunden.")
            sys.exit(1)
        self.asset_name = asset_name
        self.config = ASSETS[asset_name]
        self.base_topic = f"{UNS_BASE_TOPIC}/{self.config['bereich']}/{self.asset_name}"
        self.state = {"teilezaehler": 0}
        self.client = mqtt.Client(client_id=f"{asset_name}_sim_{random.randint(100,999)}")
        self.client.on_connect = self.on_connect
        self.client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        self.client.tls_set(tls_version=mqtt.ssl.PROTOCOL_TLS)
        self.client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"MQTT-Simulator für '{self.asset_name}' verbunden.")
        else:
            print(f"MQTT-Verbindung fehlgeschlagen, code {rc}\n")
    
    def get_sensor_value(self, sensor_name):
        # ... (kompletter Code der Funktion von vorher)
        if sensor_name == "Temperatur": return round(random.uniform(80, 95), 2), "°C"
        if sensor_name == "Druck": return round(random.uniform(5.0, 5.5), 2), "bar"
        # ... etc.
        return None, None

    def run(self):
        # ... (kompletter Code der Funktion von vorher)
        print(f"Starte MQTT Simulation für {self.asset_name}")
        # ... etc.

# --- Hauptteil des Skripts (geändert) ---
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Fehler: Bitte geben Sie den Namen der zu simulierenden Anlage an.")
        print("Verfügbare Anlagen:", ", ".join(ASSETS.keys()))
        sys.exit(1)
        
    asset_to_simulate = sys.argv[1]

    # HINZUGEFÜGT: Starte den Webserver in einem separaten Thread
    web_thread = threading.Thread(target=run_web_server)
    web_thread.daemon = True  # Erlaubt dem Hauptprogramm, zu beenden
    web_thread.start()
    print("Health Check Webserver gestartet.")

    # Starte den MQTT-Simulator im Haupt-Thread
    simulator = SensorSimulator(asset_to_simulate)
    simulator.run()