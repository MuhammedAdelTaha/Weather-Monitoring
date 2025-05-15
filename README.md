# Weather Monitoring System

---

## Table of Contents
- [Setup Instructions](#setup-instructions)

---

## Setup Instructions

### 1. **Create Docker Network**

Create a custom Docker network for the weather monitoring setup:

```bash
docker network create "weather-monitoring"
```

### 2. **Start Elasticsearch and Kibana Containers**

Start the Elasticsearch, Kibana containers and connect them to the previously created Docker network:

```bash
docker run --name elasticsearch --network "weather-monitoring" -p 9200:9200 -d -m 1GB docker.elastic.co/elasticsearch/elasticsearch:9.0.0
docker run --name kibana --network "weather-monitoring" -p 5601:5601 -d docker.elastic.co/kibana/kibana:9.0.0
```

### 3. **Retrieve Elasticsearch Credentials**

When starting Elasticsearch for the first time, generate the `elastic` password and enrollment token. 
These credentials are shown only once at startup. If you need to regenerate them, run the following commands:

* Reset the password:

  ```bash
  docker exec -it elasticsearch /usr/share/elasticsearch/bin/elasticsearch-reset-password -u elastic
  ```

* Create the enrollment token for Kibana:

  ```bash
  docker exec -it elasticsearch /usr/share/elasticsearch/bin/elasticsearch-create-enrollment-token -s kibana
  ```

### 4. **Store Elasticsearch Password as Environment Variable**

Store the `elastic` password in an environment variable for later use:

```bash
echo 'export ELASTIC_PASSWORD="your_password"' >> ~/.bashrc
source ~/.bashrc
```

### 5. **Copy SSL Certificate from Elasticsearch Container**

Copy the `http_ca.crt` SSL certificate from the Elasticsearch container to your local machine:

```bash
docker cp elasticsearch:/usr/share/elasticsearch/config/certs/http_ca.crt .
```

### 6. **Verify Elasticsearch with REST API Call**

Make a REST API call to ensure the Elasticsearch container is running:

```bash
curl --cacert http_ca.crt -u elastic:$ELASTIC_PASSWORD https://localhost:9200
```

### 7. **Complete Kibana Enrollment and Login**

* Paste the enrollment token obtained earlier into Kibana.
* Press `Enter` and then execute the following command to get the verification code:

  ```bash
  docker exec -it kibana bin/kibana-verification-code
  ```
* Copy the verification code and paste it into the Kibana enrollment prompt.
* Log into Kibana using the `elastic` username and the previously stored password.

### 8. **Create the Required Images**
cd inside the following pathes:
- "/Weather-Monitoring/Weather-Station" 
- "/Weather-Monitoring/Base-Central-Station"
- "/Weather-Monitoring/Rain-Triggers"

and run the following command in the terminal to create their images:

```bash
 mvn compile jib:dockerBuild
```

### 9. **Start Other Containers Using Docker Compose**
To simplify the management of all services (ZooKeeper, Kafka, weather stations, etc.), use the docker-compose.yaml file. 
This file includes configurations for the following services:

- **ZooKeeper:** Used for Kafka coordination.
- **Kafka:** The message broker for data streaming.
- **Weather Stations:** 10 instances of weather stations, each sending data to Kafka.

```bash
docker compose up -d
```

### 10. **Stopping Everything**
To stop all running containers, run:

```bash
docker container stop elasticsearch kibana
docker compose down
```

### 11. **Starting Everything Again**
To start all services again, run:

```bash
docker container start elasticsearch kibana
docker compose up -d
```

Then, just login with the `elastic` username and password.
