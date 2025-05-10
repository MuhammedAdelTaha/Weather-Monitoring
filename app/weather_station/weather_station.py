import json
import random
import time
import sys
import os
from confluent_kafka import Producer


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def create_weather_message(station_id, seq_number):
    """Create a weather status message with random values."""
    # Battery status with probabilities: low=30%, medium=40%, high=30%
    battery_status_options = ["low", "medium", "high"]
    battery_status_weights = [0.3, 0.4, 0.3]
    battery_status = random.choices(battery_status_options, battery_status_weights)[0]

    # Weather data with random values
    humidity = random.randint(10, 100)  # percentage
    temperature = random.randint(32, 110)  # Fahrenheit
    wind_speed = random.randint(0, 60)  # km/h

    # Create message
    message = {
        "station_id": station_id,
        "s_no": seq_number,
        "battery_status": battery_status,
        "status_timestamp": int(time.time()),
        "weather": {
            "humidity": humidity,
            "temperature": temperature,
            "wind_speed": wind_speed
        }
    }

    return message


def run_weather_station(station_id, kafka_bootstrap_servers="kafka:9092"):
    """Run the weather station simulation."""
    # Configure the Kafka producer
    conf = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'client.id': f'weather-station-{station_id}'
    }
    producer = Producer(conf)

    # Topic to send messages to
    topic = "weather-data"

    # Sequence number counter
    seq_number = 1

    print(f"Weather Station {station_id} started. Sending data to Kafka topic: {topic}")

    try:
        while True:
            # Randomly drop messages at 10% rate
            if random.random() < 0.1:
                print(f"Station {station_id} - Dropped message {seq_number}")
                continue

            # Create weather message
            message = create_weather_message(station_id, seq_number)

            # Convert message to JSON string
            message_json = json.dumps(message)

            # Send message to Kafka
            producer.produce(topic=topic, key=str(station_id), value=message_json.encode('utf-8'), callback=delivery_report)

            # Force sending of all messages
            producer.flush()

            print(f"Station {station_id} - Sent message {seq_number}: {message_json}")

            # Increment sequence number
            seq_number += 1

            # Wait for 1 second before next reading
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"Weather Station {station_id} stopping...")
    finally:
        # Clean up resources
        producer.flush()
        print(f"Weather Station {station_id} stopped")


def main():
    # Get station ID from command line or environment variable, default is 1
    station_id = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.environ.get("STATION_ID", 1))

    # Get Kafka bootstrap servers from environment variable or use default
    kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    run_weather_station(station_id, kafka_bootstrap_servers)


if __name__ == "__main__":
    main()