# Connect + Debezium + Avro + Python

- [Connect + Debezium + Avro + Python](#connect--debezium--avro--python)
  - [Start Services](#start-services)
  - [Install Debezium Postgres CDC plugin](#install-debezium-postgres-cdc-plugin)
  - [Populate the database](#populate-the-database)
  - [Create the topic](#create-the-topic)
  - [Initial Load](#initial-load)
  - [Python Protobuf Consumer](#python-protobuf-consumer)
  - [Test CDC](#test-cdc)
  - [Cleanup](#cleanup)

## Start Services

```shell
docker compose up -d
```

Monitor logs:

```shell
docker compose logs -f
```

Open Control Center: http://localhost:9021/clusters

## Install Debezium Postgres CDC plugin

```bash
docker compose exec -it connect bash
```

Once inside the container we can install a new connector from confluent-hub (and the avro connverter):

```bash
confluent-hub install --no-prompt debezium/debezium-connector-postgresql:latest
confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:latest
```

Now we need to restart our connect:

```bash
docker compose restart connect
```

Now if we list our plugins we should see a new one corresponding to the `io.debezium.connector.postgresql.PostgresConnector`.

```bash
curl localhost:8083/connector-plugins | jq
```

## Populate the database

Let's create in our postgres database (running on port 5432 with user/password `postgres/password`) a table and populate it with some data:

```sql
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO public.customers (first_name, last_name, creation_date) VALUES
('John', 'Doe', '2024-01-01 10:00:00'),
('Jane', 'Smith', '2024-01-02 11:30:00'),
('Michael', 'Johnson', '2024-01-03 12:45:00'),
('Emily', 'Williams', '2024-01-04 14:00:00'),
('Daniel', 'Brown', '2024-01-05 15:15:00'),
('Olivia', 'Jones', '2024-01-06 16:30:00'),
('Matthew', 'Garcia', '2024-01-07 17:45:00'),
('Sophia', 'Martinez', '2024-01-08 19:00:00'),
('James', 'Davis', '2024-01-09 20:15:00'),
('Ava', 'Miller', '2024-01-10 21:30:00');

SELECT * FROM customers;
```

## Create the topic

Let's create our topic:

```shell
kafka-topics --bootstrap-server localhost:9092 --topic customers --create --partitions 3 --replication-factor 1
```

## Initial Load

Now let's create our source connector:

```bash
curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://localhost:8083/connectors/pg-source-connector/config \
    -d '{
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "postgres",
            "database.password": "password",
            "database.dbname": "postgres",
            "database.server.name": "postgres",
            "table.include.list": "public.customers",
            "topic.prefix": "pg",
            "plugin.name": "pgoutput",
            "database.history.kafka.bootstrap.servers": "broker:29092",
            "database.history.kafka.topic": "schema-changes.customers",
            "slot.name": "debezium_slot",
            "publication.name": "db_publication",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schemas.enable": "false",
            "value.converter.schema.registry.url": "http://schema-registry:8081",
            "transforms": "ExtractValue,ExtractKeyFromValue,KeyToString,CreationDate,TopicRouter",    
            "transforms.ExtractValue.type": "org.apache.kafka.connect.transforms.ExtractField$Value",
            "transforms.ExtractValue.field": "after",
            "transforms.ExtractKeyFromValue.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
            "transforms.ExtractKeyFromValue.field": "customer_id",
            "transforms.KeyToString.type": "org.apache.kafka.connect.transforms.Cast$Key",
            "transforms.KeyToString.spec": "string",
            "transforms.CreationDate.field": "creation_date",
            "transforms.CreationDate.target.type": "Timestamp",
            "transforms.CreationDate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
            "transforms.CreationDate.unix.precision": "microseconds",
            "transforms.TopicRouter.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.TopicRouter.regex": "pg\\.public\\.customers",
            "transforms.TopicRouter.replacement": "customers",
            "include.schema.changes": "true",
            "snapshot.mode": "initial"}'
```

After you can check the logs:

```shell
docker compose logs -f connect
```

## Python Protobuf Consumer

Prepare your environment:

```shell
python -m venv my-venv
my-venv/bin/pip install confluent-kafka 
my-venv/bin/pip install fastavro 
my-venv/bin/pip install pytz
my-venv/bin/pip install requests
```

Run the consumer:

```shell
my-venv/bin/python consumer.py -b localhost:9092 -s http://localhost:8081 -t customers -g test1
```

## Test CDC

On the postgres database run:

```sql
INSERT INTO public.customers (first_name, last_name, creation_date) VALUES
('Rui', 'Fernandes', '2024-09-15 01:00:00');

UPDATE public.customers SET 
first_name='Juan',last_name= 'Different'
WHERE customer_id=1;

INSERT INTO public.customers (first_name, last_name, creation_date) VALUES
('Bar', 'Snir', '2024-09-15 02:00:00');
```

Check the new entries are consumed by our python consumer.

## Cleanup

```shell
docker compose down -v
```