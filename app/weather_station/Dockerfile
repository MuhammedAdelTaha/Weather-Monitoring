FROM python:3.9-slim

WORKDIR /app/weather_station

# Install dependencies
RUN pip install confluent-kafka

# Copy the weather station script
COPY "app/weather_station/weather_station.py" .

# Set default environment variables
ENV STATION_ID=1
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# Run the weather station script
CMD ["python", "weather_station.py"]