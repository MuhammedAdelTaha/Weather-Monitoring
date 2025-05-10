# Weather Monitoring System

---

## Table of Contents
- [Setup Instructions](#setup-instructions)

---

## Setup Instructions

### 1. **Create Docker Network**

Create a custom Docker network for the weather monitoring setup:

```bash
docker network create weather-monitoring-network
```

### 2. **Start Elasticsearch Container**

Start the Elasticsearch container and connect it to the previously created Docker network:

```bash
docker run --name elasticsearch --network weather-monitoring-network -p 9200:9200 -d -m 1GB docker.elastic.co/elasticsearch/elasticsearch:9.0.0
```

### 3. **Retrieve Elasticsearch Credentials**

When starting Elasticsearch for the first time, generate the `elastic` password and enrollment token. These credentials are shown only once at startup. If you need to regenerate them, run the following commands:

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

### 7. **Start Kibana Container**

Start the Kibana container and connect it to the same Docker network:

```bash
docker run --name kibana --network weather-monitoring-network -p 5601:5601 -d docker.elastic.co/kibana/kibana:9.0.0
```

### 8. **Complete Kibana Enrollment**

* Paste the enrollment token obtained earlier into Kibana.
* Press `Enter` and then execute the following command to get the verification code:

  ```bash
  docker exec -it kibana bin/kibana-verification-code
  ```

### 9. **Log into Kibana**

Log into Kibana using the `elastic` username and the previously stored password.

### 10. **Start Elasticsearch and Kibana Containers (Subsequent Runs)**

After the initial setup, to start Elasticsearch and Kibana containers, simply run:

```bash
docker container start elasticsearch kibana
```

Then log in with the `elastic` username and password.

### 11. **Start Other Containers Using Docker Compose**
To simplify the management of all services (ZooKeeper, Kafka, weather stations, etc.), use the docker-compose.yaml file. 
This file includes configurations for the following services:

- **ZooKeeper:** Used for Kafka coordination.
- **Kafka:** The message broker for data streaming.
- **Weather Stations:** 10 instances of weather stations, each sending data to Kafka.

