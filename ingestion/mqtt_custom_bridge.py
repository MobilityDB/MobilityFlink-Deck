import os
import json
import logging
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from kafka import KafkaProducer

KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',') # KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'locations_topic')
MQTT_BROKER_HOST = os.getenv('MQTT_BROKER_HOST', 'localhost')
MQTT_BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', 1883))
MQTT_TOPIC_FILTER = os.getenv('MQTT_TOPIC_FILTER', 'devices/+/location')
MQTT_CLIENT_ID = os.getenv('MQTT_CLIENT_ID', 'bridge-client')
MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger("mqtt-kafka-bridge")

# Initialisation Kafka
try:
    kafka = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
except Exception as e:
    log.error(f"Erreur init Kafka : {e}")
    exit(1)

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        log.info("MQTT connecté.")
        client.subscribe(MQTT_TOPIC_FILTER)
    else:
        log.warning(f"Connexion MQTT échouée (code {rc})")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception as e:
        log.debug(f"Payload non décodable: {e}")
        return

    vehicle_id = payload.get("vehicleID")
    if not vehicle_id:
        parts = msg.topic.split("/")
        if len(parts) >= 2 and parts[0] == "devices":
            vehicle_id = parts[1]

    if not vehicle_id:
        log.debug(f"vehicleID manquant pour le topic {msg.topic}")
        return

    message = {
        "vehicleID": str(vehicle_id),
        "latitude": payload.get("latitude"),
        "longitude": payload.get("longitude"),
        "timestamp": payload.get("timestamp"),
        "source": "mqtt"
    }
    kafka.send(KAFKA_TOPIC, value=message)




def on_disconnect(client, userdata, rc, properties=None):
    if rc != 0:
        log.info("Déconnexion MQTT inattendue.")

client = mqtt.Client(client_id=MQTT_CLIENT_ID, callback_api_version=CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

if MQTT_USERNAME:
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

try:
    client.connect_async(MQTT_BROKER_HOST, MQTT_BROKER_PORT)
    client.loop_forever()
except KeyboardInterrupt:
    log.info("Interruption par l'utilisateur")
except Exception as e:
    log.error(f"Erreur principale : {e}")
