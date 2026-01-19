![Python][python-url]
![Docker][docker-url]
![Postgres][postgres-url]
![MinIO][minio-url]
![PyProject][pyproject-url]
![Airflow][airflow-url]

# Mini data platform for CDC

## About The Project

---

Mini Data Platform is a containerized, end-to-end data processing pipeline 
designed to demonstrate real-time Change Data Capture (CDC) using Debezium, 
Apache Kafka, and PostgreSQL. It captures database changes, serializes them with Avro,
and streams them via Kafka. A PySpark consumer decodes these messages and stores them in MinIO
using the Delta Lake format, enabling ACID-compliant, versioned, and scalable data storage.
The platform includes additional tools like Kafka UI for stream monitoring and Schema Registry 
for schema management, making it ideal for prototyping modern data lakehouse architectures.

### Built With

---

* ![Python][python-url]
* ![SQLAlchemy][sqlalchemy-url]
* ![Docker][docker-url]
* ![Postgres][postgres-url]
* ![Kafka][kafka-url]
* ![Kafka UI][kafka-ui-url]
* ![Kafka Connect][kafka-connect-url]
* ![Schema Registry][schema-registry-url]
* ![MinIO][minio-url]
* ![PyProject][pyproject-url]
* ![Kafka Consumer][kafka-consumer-url]




<!-- GETTING STARTED -->
## Getting Started

---

If you want to try out **Mini Data Platform** please read this section. It will guide you step by step on how to run this project.

### Prerequisites

---

You need to have docker installed on you machine. This documentation uses docker compose V2 syntax, so make sure you have installed **docker compose plugin**.

* Windows:

    To install docker on windows download and install [docker desktop](https://www.docker.com/products/docker-desktop/). Check out docker desktop documentation for more info [https://docs.docker.com/desktop/setup/install/windows-install/](https://docs.docker.com/desktop/setup/install/windows-install/)

* Linux:

    Docker installation for Linux is complex. Please follow docker documentation on [Install Docker Desktop on Linux](https://docs.docker.com/desktop/setup/install/linux/).

* Mac:

    Docker installation for Linux is complex. Please follow docker documentation on [Install Docker Desktop on Mac](https://docs.docker.com/desktop/setup/install/mac-install/).

### How to start

---

Since whole app is distributed and containerized you can start application with **docker** using _docker-compose.yml_ file.
 You must replace all <\envs> with your values. Check out [PyProject README](./PyProject/README.md) and [KafkaConsumer README](./KafkaConsumer/README.md) for required environment variables. 

```yaml
networks:
  default:
    driver: bridge

services:
  pyproject:
    image: martynakuk/mini-data-platform:pyproject
    environment:
      <PyProject required envs>
    networks:
      - default
    depends_on:
      - postgres
      - connect

  consumer1:
    image: martynakuk/mini-data-platform:consumer1
    environment:
      <KafkaConsumer required envs>
    networks:
      - default
    depends_on:
      - kafka

  postgres:
    image: postgres:14.7
    command: |
      postgres 
      -c wal_level=logical
    environment:
      POSTGRES_DB: <your_database_name>
      POSTGRES_USER: <your_database_user>
      POSTGRES_PASSWORD: <your_database_password>
    networks:
      - default
    ports:
      - 5432:5432

  kafka:
    image: quay.io/debezium/kafka
    environment:
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_LISTENERS: INTERNAL://kafka:9092,CONTROLLER://kafka:9093,EXTERNAL://0.0.0.0:9099
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://kafka:9099
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_BROKER_ID: 1
      NODE_ID: 1
      CLUSTER_ID: 1
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    networks:
      - default
    ports:
      - 9099:9099

  connect:
    image: quay.io/debezium/connect
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      CONNECT_TOPIC_CREATION_ENABLE: "true"
      CONNECT_PRODUCER_COMPRESSION_TYPE: snappy
      CONNECT_KEY_CONVERTER: io.confluent.connect.avro.AvroConverter
      CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL: 'http://schemareg:8081'
      CONNECT_VALUE_CONVERTER: io.confluent.connect.avro.AvroConverter
      CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: 'http://schemareg:8081'
      CONNECT_INTERNAL_KEY_CONVERTER: io.confluent.connect.avro.AvroConverter
      CONNECT_INTERNAL_VALUE_CONVERTER: io.confluent.connect.avro.AvroConverter
      CONNECT_REST_ADVERTISED_HOST_NAME: connect
      CONNECT_EXACTLY_ONCE_SOURCE_SUPPORT: enabled
      CONNECT_ENABLE_DEBEZIUM_KC_REST_EXTENSION: "true"
      CONNECT_ENABLE_DEBEZIUM_SCRIPTING: "true"
      GROUP_ID: 2
      CONFIG_STORAGE_TOPIC: dbz_connect_dev_configs
      OFFSET_STORAGE_TOPIC: dbz_connect_dev_offsets
      STATUS_STORAGE_TOPIC: dbz_connect_dev_statuses
    networks:
      - default
    ports:
      - 8083:8083
    volumes:
      - ./jars:/kafka/connect/libs
    depends_on:
      - kafka
      - postgres

  schemareg:
    image: confluentinc/cp-schema-registry
    environment:
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
      SCHEMA_REGISTRY_HOST_NAME: schemareg
      SCHEMA_REGISTRY_LISTENERS: http://schemareg:8081
    networks:
      - default
    ports:
      - 8081:8081

  kafka-ui:
    image: provectuslabs/kafka-ui
    environment:
      AUTH_TYPE: LOGIN_FORM
      SPRING_SECURITY_USER_NAME: <your_username>
      SPRING_SECURITY_USER_PASSWORD: <your_password>
      DYNAMIC_CONFIG_ENABLED: 'true'
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schemareg:8081
      KAFKA_CLUSTERS_0_NAME: CDC
    networks:
      - default
    ports:
      - 8080:8080

  minio:
    image: quay.io/minio/minio
    environment:
      MINIO_ROOT_USER: <your_username>
      MINIO_ROOT_PASSWORD: <your_password>
    networks:
      - default
    ports:
      - 9000:9000
      - 9001:9001
    command: server /data --console-address ":9001"

```

Next just run:

```shell
docker compose up -d
```



<!-- MARKDOWN LINKS & IMAGES -->
[python-url]: https://img.shields.io/badge/Python-3.9%2B-blue?logo=python&logoColor=white
[sqlalchemy-url]: https://img.shields.io/badge/SQLAlchemy-ORM-red?logo=sqlalchemy&logoColor=white
[docker-url]: https://img.shields.io/badge/Docker-Containerized-blue?logo=docker&logoColor=white
[postgres-url]: https://img.shields.io/badge/PostgreSQL-13-blue?logo=postgresql&logoColor=white
[kafka-url]: https://img.shields.io/badge/Kafka-Debezium-green?logo=apache-kafka&logoColor=white
[kafka-ui-url]: https://img.shields.io/badge/Kafka_UI-CDC_Monitoring-purple?logo=apache-kafka&logoColor=white
[kafka-connect-url]: https://img.shields.io/badge/Kafka_Connect-Debezium-orange?logo=apache-kafka&logoColor=white
[kafka-consumer-url]: https://img.shields.io/badge/Kafka_Consumer-Avro_Decoder-brightgreen?logo=apache-kafka&logoColor=white
[schema-registry-url]: https://img.shields.io/badge/Schema_Registry-Confluent-blue?logo=confluent&logoColor=white
[minio-url]: https://img.shields.io/badge/MinIO-Cloud_Storage-yellow?logo=minio&logoColor=white
[pyproject-url]: https://img.shields.io/badge/PyProject-Kafka_to_MinIO-success?logo=python&logoColor=white