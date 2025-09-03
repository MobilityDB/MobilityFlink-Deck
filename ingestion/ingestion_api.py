import os
import json
import logging
from flask import Flask, request, jsonify
from kafka import KafkaProducer

# Configuration
KAFKA_BROKERS = os.environ.get('KAFKA_BROKERS', 'localhost:9092').split(',')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'locations_topic')
FLASK_HOST = os.environ.get('FLASK_HOST', '0.0.0.0')
FLASK_PORT = int(os.environ.get('FLASK_PORT', '5000'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3
    )
    logger.info("Connected to Kafka successfully")
except Exception as e:
    logger.critical(f"Failed to connect to Kafka: {e}")
    exit(1)

app = Flask(__name__)

@app.route('/ingest/location', methods=['POST'])
def ingest_locations():

    if not request.is_json:
        return jsonify({"error": "Expected JSON body"}), 400
    
    data = request.get_json()
    
    if not isinstance(data, list):
        return jsonify({"error": "Expected a list of vehicles"}), 400
    
    points_sent = 0
    errors = []
    
    for vehicle_data in data:
        if not isinstance(vehicle_data, dict):
            continue
        
        vehicle_id = vehicle_data.get("vehicleID")
        path = vehicle_data.get("path")
        timestamps = vehicle_data.get("timestamps")
        
        if not all([
            vehicle_id is not None,
            isinstance(path, list),
            isinstance(timestamps, list),
            len(path) == len(timestamps)
        ]):
            errors.append(f"Invalid structure for vehicle {vehicle_id}")
            continue
        
        for coords, ts in zip(path, timestamps):
            try:
                message = {
                    "vehicleID": vehicle_id,
                    "latitude": coords[1] if isinstance(coords, list) and len(coords) >= 2 else None,
                    "longitude": coords[0] if isinstance(coords, list) and len(coords) >= 2 else None,
                    "timestamp": ts,
                    "source": "http_batch"
                }
                
                producer.send(KAFKA_TOPIC, value=message)
                points_sent += 1
                
            except Exception as e:
                errors.append(f"Kafka send error for vehicle {vehicle_id}: {str(e)}")
                continue
    
    if points_sent > 0:
        try:
            producer.flush()
            logger.info(f"Sent {points_sent} raw points to Kafka for processing")
            return jsonify({
                "status": "ok", 
                "sent": points_sent,
                "message": "Points sent to processing pipeline"
            }), 202
        except Exception as e:
            logger.error(f"Kafka flush error: {e}")
            return jsonify({"error": "Failed to send to Kafka", "details": str(e)}), 500
    
    return jsonify({
        "status": "warning",
        "message": "No valid data structure found",
        "errors": errors[:5]
    }), 400


if __name__ == '__main__':
    logger.info(f"Starting ingestion API on {FLASK_HOST}:{FLASK_PORT}")
    logger.info(f"Sending data to Kafka topic: {KAFKA_TOPIC}")
    logger.info("All validations will be performed by Flink")
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=False)