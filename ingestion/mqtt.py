#!/usr/bin/env python3

import os
import json
import logging
import signal
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "locations_topic")

MQTT_HOST      = os.getenv("MQTT_BROKER_HOST", "localhost")
MQTT_PORT      = int(os.getenv("MQTT_BROKER_PORT", "1883"))

MQTT_FILTERS   = [
    t.strip() for t in os.getenv(
        "MQTT_TOPIC_FILTERS",
        "owntracks/+/+,devices/+/location"
    ).split(",") if t.strip()
]
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "mqtt-kafka-bridge")
MQTT_USERNAME  = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD  = os.getenv("MQTT_PASSWORD")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("mqtt-bridge")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    linger_ms=10,
    retries=5
)

def to_msg(vehicle_id, lat, lon, ts, source):
    return {
        "vehicleID": None if vehicle_id is None else str(vehicle_id),
        "latitude":  lat,
        "longitude": lon,
        "timestamp": ts,
        "source":    source
    }

def vehicle_id_from_topic(topic: str) -> str | None:
    """
      - owntracks/{user}/{device}
      - devices/{id}/...
    """
    parts = topic.split("/")
    if topic.startswith("owntracks/") and len(parts) >= 3:
        return parts[2]
    if len(parts) >= 2:
        return parts[1]
    return None

def map_payload(topic: str, payload: dict) -> dict | None:
    """
    - OwnTracks : lat/lon/tst (ou timestamp)
    - Custom    : latitude/longitude/timestamp
    """
    if not isinstance(payload, dict):
        return None

    # 1) OwnTracks
    if topic.startswith("owntracks/"):
        dev_id = vehicle_id_from_topic(topic)
        return to_msg(
            dev_id,
            payload.get("lat") or payload.get("latitude"),
            payload.get("lon") or payload.get("longitude"),
            payload.get("tst") or payload.get("timestamp"),
            "mqtt-owntracks"
        )

    # 2) Custom
    vid = payload.get("vehicleID")
    if not vid:
        vid = vehicle_id_from_topic(topic)

    return to_msg(
        vid,
        payload.get("latitude"),
        payload.get("longitude"),
        payload.get("timestamp"),
        payload.get("source") or "mqtt"
    )


client = mqtt.Client(client_id=MQTT_CLIENT_ID)
if MQTT_USERNAME:
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

def on_connect(c, u, flags, rc):
    if rc == 0:
        log.info("MQTT connected %s:%s", MQTT_HOST, MQTT_PORT)
        for f in MQTT_FILTERS:
            c.subscribe(f)
            log.info("Subscribed: %s", f)
    else:
        log.warning("MQTT connect failed rc=%s", rc)

def on_message(c, u, msg):
    payload = json.loads(msg.payload.decode("utf-8"))

    out = map_payload(msg.topic, payload)
    if out is None:
        return

    producer.send(KAFKA_TOPIC, value=out)


def on_disconnect(c, u, rc):
    if rc != 0:
        log.warning("MQTT disconnected unexpectedly (rc=%s)", rc)

client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect




def shutdown(*_):
    log.info("Shutting down…")

    client.loop_stop()
    client.disconnect()

    try:
        producer.flush(5)
        producer.close(5)
    except Exception:
        pass

if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    client.connect_async(MQTT_HOST, MQTT_PORT)
    client.loop_start()

    log.info(
        "Bridging MQTT→Kafka | topic=%s | filters=%s",
        KAFKA_TOPIC, MQTT_FILTERS
    )

    try:
        signal.pause()
    except KeyboardInterrupt:
        shutdown()
