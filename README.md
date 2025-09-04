# Real time visualization 

## Demo

![Demo animation](./demo.gif)


## Architecture (ingestion)

The ingestion part groups the ingestion scripts that feed Kafka from different sources (HTTP batch and MQTT). They produce geolocated messages ready to be consumed by the pipeline (Flink, MobilityDB/PostGIS, WebSocket to Deck.gl).
```
[Clients HTTP/JSON] ──> Flask API (/ingest/location) ──> Kafka (locations_topic)
                                     
[Datasets (OpenSky, … )] ─ dataset_loader.py  ──> Kafka (locations_topic)
                                     
[Appareils / IoT / OwnTracks] ──> MQTT ──> Kafka (locations_topic)
```

## Prerequisites

- Python 3.10+
- Access to a Kafka cluster (e.g. `localhost:9092`)
- (Optional) An MQTT broker (e.g. `localhost:1883`) if using MQTT bridges

Install dependencies:

```bash
pip install flask kafka-python paho-mqtt requests
```


### 1) HTTP API — batch ingestion

Runs the Flask API that accepts lists of objects with their trajectories, then pushes each point to Kafka.

Environment variables:

| Variable        | Default           | Description                               |
|-----------------|----------------------|-------------------------------------------|
| `KAFKA_BROKERS` | `localhost:9092`     | Liste de brokers Kafka                    |
| `KAFKA_TOPIC`   | `locations_topic`    | Topic de sortie                           |
| `FLASK_HOST`    | `0.0.0.0`            | Listening host                             |
| `FLASK_PORT`    | `5000`               | HTTP port                                 |

Execution (export optional if default values are already present in the implementation):

```bash
export KAFKA_BROKERS="localhost:9092"
export KAFKA_TOPIC="locations_topic"
export FLASK_PORT=5000

python http_ingestion_api.py
```

Example request (historical mode must be enabled in the visualization to see old data):

```bash
curl -X POST "http://localhost:5000/ingest/location"   -H "Content-Type: application/json"   -d '[{
        "vehicleID": "plane-001",
        "path": [[1.1543,56.74],[1.1643,56.84]],
        "timestamps": [1693564800, 1693568400]
      }]'
```

Expected response (202):

```json
{"status":"ok","sent":2,"message":"Points sent to processing pipeline"}
```

### 1.1) MQTT → Kafka — **advanced** bridge (OwnTracks + custom)

Key environment variables:

| Variable               | Default                                  | Exemple                         |
|------------------------|----------------------------------------------|----------------------------------|
| `KAFKA_BROKERS`        | `localhost:9092`                            | `broker1:9092,broker2:9092`     |
| `KAFKA_TOPIC`          | `locations_topic`                           | `locations_topic`               |
| `MQTT_BROKER_HOST`     | `localhost`                                 | `mqtt.example.org`              |
| `MQTT_BROKER_PORT`     | `1883`                                      | `8883`                          |
| `MQTT_TOPIC_FILTERS`   | `owntracks/+/+,devices/+/location`          | `sensors/+/gps,owntracks/+/+`   |
| `MQTT_CLIENT_ID`       | `mqtt-kafka-bridge`                         | `bridge-prod-1`                 |
| `MQTT_USERNAME`        | *(vide)*                                    | `user1`                         |
| `MQTT_PASSWORD`        | *(vide)*                                    | `****`                          |

Execution:

```bash
python mqtt.py
```

The bridge:
- subscribes to multiple filters (e.g. OwnTracks and/or your business topics);
- extracts `vehicleID` from the topic if missing from the payload (e.g. `owntracks/{user}/{device}`, `devices/{id}/...`);
- automatically maps OwnTracks payloads (`lat`, `lon`, `tst`) to the common schema.

### 1.2) MQTT → Kafka — **custom** bridge (only custom)

Environment variables:

| Variable             | Default            |
|----------------------|-----------------------|
| `KAFKA_BROKERS`      | `localhost:9092`      |
| `KAFKA_TOPIC`        | `locations_topic`     |
| `MQTT_BROKER_HOST`   | `localhost`           |
| `MQTT_BROKER_PORT`   | `1883`                |
| `MQTT_TOPIC_FILTER`  | `devices/+/location`  |
| `MQTT_CLIENT_ID`     | `bridge-client`       |
| `MQTT_USERNAME`      | *vide*                |
| `MQTT_PASSWORD`      | *vide*                |

Execution:

```bash
python mqtt_custom_bridge.py
```

### 1.3) Sending datasets — `dataset_loader.py`

Reads a JSON file, groups by `vehicleID`, and sends it to the HTTP API (see point 1).

Variables & options:
- `API_URL` (modifiez dans le script si nécessaire, par default `http://localhost:5000/ingest/location`)
- `batch_size` (dans le script, par default `500`)

Usage :

```bash
python dataset_loader.py states_2018-05-28-00.json
```

Output:
```
reading ./data/dataset_sample.json...
processing 12345 flights...
sending batch 1 (500 objects)...
sending batch 2 (500 objects)...
sending final batch 3 (123 objects)...
done. total points sent: 98765
```

## Input contracts (HTTP)

**Request (list of objects):**
```json
[
  {
    "vehicleID": "plane-001",
    "path": [[lon, lat], [lon, lat], ...],
    "timestamps": [unix_ts_1, unix_ts_2, ...]
  }
]
```

## Dev

- Activate a venv and install the packages listed above.
- Run Kafka locally and create `locations_topic`.




## 2) Flink 2.0 (Kafka → MobilityDB & Kafka WS)

This **PyFlink Table API** job consumes the Kafka ingestion topic, cleans the records, then:
- **inserts** each point (TGeogPoint) into **MobilityDB/Postgresql** via a **JDBC** sink;
- **also publishes** a minimal JSON message to a second Kafka topic intended for the **WebSocket server**.

### Script
- File: `flink.py` 
- Sinks → `jdbc_point_event_sink` (table `vehicle_locations_temporal_insert_view`) et `kafka_sink_ws`

### Database initialization (to be executed before Flink)

Before running the Flink job, **execute the SQL script** that creates/initializes the required objects (tables/views/insert view) in PostgreSQL/MobilityDB.

- Current file: `flink_tables.sql`


### Required connectors (Flink 2.0)
The connectors **are not included** add the corresponding JARs into `$FLINK_HOME/lib`:
- **Kafka SQL connector** : `flink-connector-kafka` (version **4.0.x-2.0**, ex. `4.0.0-2.0`)
- **JDBC SQL connector (core + Postgres)** : `flink-connector-jdbc-core` et `flink-connector-jdbc-postgres` (version **4.0.x-2.0**)

Useful links:
- **Apache Flink 2.0.0** : https://downloads.apache.org/flink/flink-2.0.0/
- **Kafka SQL connector** : https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka/4.0.0-2.0
- **JDBC postgresql connector** : https://mvnrepository.com/artifact/org.apache.flink/flink-connector-jdbc-postgres/4.0.0-2.0
- **JDBC core connector** : https://mvnrepository.com/artifact/org.apache.flink/flink-connector-jdbc-core/4.0.0-2.0

### Environment variables (to be adapted)
- `KAFKA_BROKERS` (default `localhost:9092`)
- `KAFKA_TOPIC` (source, par default `locations_topic`)
- `KAFKA_WEBSOCKET_TOPIC` (sink, default `websocket_updates_topic`)
- `JDBC_URL` (ex. `jdbc:postgresql://localhost:5432/mobilityTest`) 
- `JDBC_USERNAME`, `JDBC_PASSWORD`, `JDBC_DRIVER` (ex. `org.postgresql.Driver`)

### Local execution (example)
```bash
# 1) Start a Flink cluster
$FLINK_HOME/bin/start-cluster.sh

# 2) (Optional) Activate a venv & compatible PyFlink
python -m venv .venv && source .venv/bin/activate
pip install 'apache-flink==2.0.*'

# 4) Soumettre le job PyFlink (export optionel si variable par default)
export KAFKA_BROKERS="localhost:9092"
export KAFKA_TOPIC="locations_topic"
export KAFKA_WEBSOCKET_TOPIC="websocket_updates_topic"
export JDBC_URL="jdbc:postgresql://localhost:5432/mobilityTest"
export JDBC_USERNAME="postgres"
export JDBC_PASSWORD="postgres"
export JDBC_DRIVER="org.postgresql.Driver"

$FLINK_HOME/bin/flink run -py flink.py
```

### Validation & throughput
- The job applies **UDFs** for normalization (`vehicleID`, multi-unit timestamps) and **coordinate validations**.
- **Watermarks** basés sur l’event-time avec un **retard** configuré (par default 10 s) pour tolérer un léger retard réseau.
- **Flush JDBC**: buffer configured to balance latency and throughput.

**Output (JDBC → MobilityDB)**: insertion of `(vehicle_id, timestamp_ms, tgeogpoint_text)` into `vehicle_locations_temporal_insert_view`.





## 3) WebSocket server
The server reads the Kafka topic **`websocket_updates_topic`** produced by Flink and relays each message to connected clients

**Dependencies** : `websockets`, `aiokafka`

### Environment variables
| Variable                | Default                | Description                              |
|-------------------------|---------------------------|------------------------------------------|
| `KAFKA_BROKERS`         | `localhost:9092`          | Brokers Kafka          
| `KAFKA_WEBSOCKET_TOPIC` | `websocket_updates_topic` | Topic consommé par le serveur WS 
| `KAFKA_GROUP_ID`        | *(auto)*                  | (Optionnel) GroupId du consumer Kafka
| `WEBSOCKET_HOST`        | `0.0.0.0`                 | Listening host 
| `WEBSOCKET_PORT`        | `8082`                    | Listening port

### Installation

```bash
pip install websockets aiokafka

export KAFKA_BROKERS="localhost:9092"
export KAFKA_WEBSOCKET_TOPIC="websocket_updates_topic"
export WEBSOCKET_HOST="0.0.0.0"
export WEBSOCKET_PORT=8082

python websocket_server.py
# The server listens on 0.0.0.0:8082
```




## 4) Deck.gl
The Deck.gl application connects to the **WebSocket** to receive updates and display them


### Installation & run
```
npm install
npm start
```

