
import json
import random
import time
import os
import logging
from datetime import datetime
from confluent_kafka import Producer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
TOPIC = 'gps-events'

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')
        pass

def generate_gps_event(vehicle_id):
    """
    Generates a simulated GPS event for a vehicle.
    """
    return {
        "vehicle_id": vehicle_id,
        "timestamp": datetime.now().isoformat(), # Fixed deprecation
        "latitude": 51.5074 + random.uniform(-0.01, 0.01),  # London approx
        "longitude": -0.1278 + random.uniform(-0.01, 0.01),
        "speed": random.uniform(0, 50),
        "heading": random.uniform(0, 360),
        "route_id": f"route_{random.randint(1, 100)}"
    }

def main():
    conf = {'bootstrap.servers': KAFKA_BROKER}
    producer = Producer(conf)

    logger.info(f"Starting GPS Producer to {KAFKA_BROKER}...")

    vehicle_ids = [f"v_{i}" for i in range(1, 501)] # 500 vehicles

    try:
        while True:
            for vid in vehicle_ids:
                event = generate_gps_event(vid)
                producer.produce(TOPIC, json.dumps(event).encode('utf-8'), callback=delivery_report)
            
            producer.poll(0)
            time.sleep(1) # Simple loop for now
            
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
