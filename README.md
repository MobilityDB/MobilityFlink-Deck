# Visualisation real time

Ce répertoire regroupe les scripts d’ingestion qui alimentent Kafka à partir de différentes sources (HTTP batch et MQTT). Ils produisent des messages géolocalisés prêts à être consommés par la pipeline (Flink, MobilityDB/PostGIS, WebSocket vers Deck.gl).

## Demo

![Demo animation](./demo.gif)


## Architecture (ingestion)

```
[Clients HTTP/JSON] ──> Flask API (/ingest/location) ──> Kafka (locations_topic)
                                     
[Datasets (OpenSky, … )] ─ dataset_loader.py  ──> Kafka (locations_topic)
                                     
[Appareils / IoT / OwnTracks] ──> MQTT ──> Kafka (locations_topic)
```

## Prérequis

- Python 3.10+
- Accès à un cluster Kafka (ex. `localhost:9092`)
- (Optionnel) Un broker MQTT (ex. `localhost:1883`) si utilisation de passerelles MQTT

Installez les dépendances :

```bash
pip install flask kafka-python paho-mqtt requests
```




### 1) API HTTP — ingestion batch

Lance l’API Flask qui accepte des listes d'objets avec leurs trajectoires, puis pousse chaque point sur Kafka.

Variables d’environnement :

| Variable        | Par défaut           | Description                               |
|-----------------|----------------------|-------------------------------------------|
| `KAFKA_BROKERS` | `localhost:9092`     | Liste de brokers Kafka                    |
| `KAFKA_TOPIC`   | `locations_topic`    | Topic de sortie                           |
| `FLASK_HOST`    | `0.0.0.0`            | Hôte d’écoute                             |
| `FLASK_PORT`    | `5000`               | Port HTTP                                 |

Exécution (export optionnel si valeur par défaut déjà présente dans l'implémentation):

```bash
export KAFKA_BROKERS="localhost:9092"
export KAFKA_TOPIC="locations_topic"
export FLASK_PORT=5000

python http_ingestion_api.py
```

Ex de requête (le mode historique doit être activé dans la visualisation pour voir d'anciennes données):

```bash
curl -X POST "http://localhost:5000/ingest/location"   -H "Content-Type: application/json"   -d '[{
        "vehicleID": "plane-001",
        "path": [[1.1543,56.74],[1.1643,56.84]],
        "timestamps": [1693564800, 1693568400]
      }]'
```

Réponse attendue (202) :

```json
{"status":"ok","sent":2,"message":"Points sent to processing pipeline"}
```

### 1.1) MQTT → Kafka — bridge **avancé** (OwnTracks + custom)

Variables d’environnement clés :

| Variable               | Par défaut                                  | Exemple                         |
|------------------------|----------------------------------------------|----------------------------------|
| `KAFKA_BROKERS`        | `localhost:9092`                            | `broker1:9092,broker2:9092`     |
| `KAFKA_TOPIC`          | `locations_topic`                           | `locations_topic`               |
| `MQTT_BROKER_HOST`     | `localhost`                                 | `mqtt.example.org`              |
| `MQTT_BROKER_PORT`     | `1883`                                      | `8883`                          |
| `MQTT_TOPIC_FILTERS`   | `owntracks/+/+,devices/+/location`          | `sensors/+/gps,owntracks/+/+`   |
| `MQTT_CLIENT_ID`       | `mqtt-kafka-bridge`                         | `bridge-prod-1`                 |
| `MQTT_USERNAME`        | *(vide)*                                    | `user1`                         |
| `MQTT_PASSWORD`        | *(vide)*                                    | `****`                          |

Exécution :

```bash
python mqtt.py
```

Le bridge :
- s’abonne à plusieurs filtres (ex. OwnTracks et/ou vos topics métier) ;
- extrait `vehicleID` depuis le topic si absent du payload (p. ex. `owntracks/{user}/{device}`, `devices/{id}/...`) ;
- mappe automatiquement les payloads OwnTracks (`lat`, `lon`, `tst`) vers le schéma commun.

### 1.2) MQTT → Kafka — bridge **custom** (only custom)

Variables d’environnement :

| Variable             | Par défaut            |
|----------------------|-----------------------|
| `KAFKA_BROKERS`      | `localhost:9092`      |
| `KAFKA_TOPIC`        | `locations_topic`     |
| `MQTT_BROKER_HOST`   | `localhost`           |
| `MQTT_BROKER_PORT`   | `1883`                |
| `MQTT_TOPIC_FILTER`  | `devices/+/location`  |
| `MQTT_CLIENT_ID`     | `bridge-client`       |
| `MQTT_USERNAME`      | *vide*                |
| `MQTT_PASSWORD`      | *vide*                |

Exécution :

```bash
python mqtt_custom_bridge.py
```

### 1.3) Envoi de datasets — `dataset_loader.py`

Lit un JSON, regroupe par `vehicleID`, et envoie vers l’API HTTP (voir point 1).

Variables & options :
- `API_URL` (modifiez dans le script si nécessaire, par défaut `http://localhost:5000/ingest/location`)
- `batch_size` (dans le script, par défaut `500`)

Usage :

```bash
python dataset_loader.py states_2018-05-28-00.json
```

Output :
```
reading ./data/dataset_sample.json...
processing 12345 flights...
sending batch 1 (500 objects)...
sending batch 2 (500 objects)...
sending final batch 3 (123 objects)...
done. total points sent: 98765
```

## Contrats d’entrée (HTTP)

**Requete (liste d'objets) :**
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

- Activez un venv et installez les paquets listés plus haut.
- Exécutez Kafka en local et créez `locations_topic`.




## 2) Flink 2.0 (Kafka → MobilityDB & Kafka WS)

Ce job **PyFlink Table API** consomme le topic Kafka d’ingestion, nettoie les enregistrements, ensuite :
- **insère** chaque point (TGeogPoint) dans **MobilityDB/Postgresql** via un sink **JDBC** ;
- **publie** aussi un message JSON minimal vers un second topic Kafka destiné au **WebSocket server**.

### Script
- Fichier : `flink.py` 
- Sinks → `jdbc_point_event_sink` (table `vehicle_locations_temporal_insert_view`) et `kafka_sink_ws`

### Initialisation base de données (a executer avant Flink)

Avant de lancer le job Flink, **exécutez le script SQL** qui crée/initialise les objets nécessaires (tables/vues/insert view) dans PostgreSQL/MobilityDB.

- Fichier actuel : `flink_tables.sql`


### Connecteurs requis (Flink 2.0)
Les connecteurs **ne sont pas inclus** ajoutez les JARs correspondants dans `$FLINK_HOME/lib` :
- **Kafka SQL connector** : `flink-connector-kafka` (version **4.0.x-2.0**, ex. `4.0.0-2.0`)
- **JDBC SQL connector (core + Postgres)** : `flink-connector-jdbc-core` et `flink-connector-jdbc-postgres` (version **4.0.x-2.0**)

Liens utile:
- **Apache Flink 2.0.0** : https://downloads.apache.org/flink/flink-2.0.0/
- **Kafka SQL connector** : https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka/4.0.0-2.0
- **JDBC postgresql connector** : https://mvnrepository.com/artifact/org.apache.flink/flink-connector-jdbc-postgres/4.0.0-2.0
- **JDBC core connector** : https://mvnrepository.com/artifact/org.apache.flink/flink-connector-jdbc-core/4.0.0-2.0

### Variables d’environnement (a adapter)
- `KAFKA_BROKERS` (défaut `localhost:9092`)
- `KAFKA_TOPIC` (source, par défaut `locations_topic`)
- `KAFKA_WEBSOCKET_TOPIC` (sink, defaut `websocket_updates_topic`)
- `JDBC_URL` (ex. `jdbc:postgresql://localhost:5432/mobilityTest`) 
- `JDBC_USERNAME`, `JDBC_PASSWORD`, `JDBC_DRIVER` (ex. `org.postgresql.Driver`)

### Lancement en local (exemple)
```bash
# 1) Démarrer un cluster Flink
$FLINK_HOME/bin/start-cluster.sh

# 2) (Optionnel) Activer un venv & PyFlink compatible
python -m venv .venv && source .venv/bin/activate
pip install 'apache-flink==2.0.*'

# 4) Soumettre le job PyFlink (export optionel si variable par défaut)
export KAFKA_BROKERS="localhost:9092"
export KAFKA_TOPIC="locations_topic"
export KAFKA_WEBSOCKET_TOPIC="websocket_updates_topic"
export JDBC_URL="jdbc:postgresql://localhost:5432/mobilityTest"
export JDBC_USERNAME="postgres"
export JDBC_PASSWORD="postgres"
export JDBC_DRIVER="org.postgresql.Driver"

$FLINK_HOME/bin/flink run -py flink.py
```

### Validation & débit
- Le job applique des **UDFs** de normalisation (`vehicleID`, timestamp multi-unités) et **validations** de coordonnées.
- **Watermarks** basés sur l’event-time avec un **retard** configuré (par défaut 10 s) pour tolérer un léger retard réseau.
- **Flush JDBC** : tampon configuré afin d’équilibrer latence et débit.

**Sortie (JDBC → MobilityDB)** : insertion de `(vehicle_id, timestamp_ms, tgeogpoint_text)` dans `vehicle_locations_temporal_insert_view`.





## 3) Serveur WebSocket
Le serveur lit le topic Kafka **`websocket_updates_topic`** produit par Flink et relaye chaque message aux clients connectés

**Dépendances** : `websockets`, `aiokafka`

### Variables d’environnement
| Variable                | Par défaut                | Description                              |
|-------------------------|---------------------------|------------------------------------------|
| `KAFKA_BROKERS`         | `localhost:9092`          | Brokers Kafka          
| `KAFKA_WEBSOCKET_TOPIC` | `websocket_updates_topic` | Topic consommé par le serveur WS 
| `KAFKA_GROUP_ID`        | *(auto)*                  | (Optionnel) GroupId du consumer Kafka
| `WEBSOCKET_HOST`        | `0.0.0.0`                 | Hôte d’écoute 
| `WEBSOCKET_PORT`        | `8082`                    | Port d’écoute

### Installation

```bash
pip install websockets aiokafka

export KAFKA_BROKERS="localhost:9092"
export KAFKA_WEBSOCKET_TOPIC="websocket_updates_topic"
export WEBSOCKET_HOST="0.0.0.0"
export WEBSOCKET_PORT=8082

python websocket_server.py
# Le serveur écoute sur ws://0.0.0.0:8082
```




## 4) Deck.gl
L’application Deck.gl se connecte au **WebSocket** pour recevoir les mises à jour et les affich


### Installation & lancement
```
npm install
npm start
```

