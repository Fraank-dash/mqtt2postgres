# System Architecture

This document explains the structural model behind the local MQTT-to-Postgres bench.

It focuses on:

- the Python publisher and Python subscriber apps
- the MQTT broker as the transport boundary
- the Postgres/TimescaleDB database as the persistence and derivation boundary
- the two ingest functions:
  - `mqtt_ingest.ingest_message(...)`
  - `mqtt_ingest.ingest_topics(...)`

## Component Model

The repo has two canonical Python app packages:

- `apps.publisher`: synthetic MQTT publishers and aggregate-driven twin-config generation
- `apps.subscriber`: MQTT subscriber runtime that forwards messages into database ingest functions

Shared packages:

- `broker.subscriber`: shared MQTT subscriber client helpers
- `observability`: runtime logging and trace payload helpers
- `mqtt2postgres`: package metadata and the subscriber module entrypoint bridge

## Main Runtime Chain

At runtime, the primary chain is:

1. a Python publisher app generates payloads
2. the MQTT broker accepts and distributes the published message
3. a Python subscriber app receives the matching topic
4. the subscriber calls one database ingest function
5. the database persists raw or topic-overview state
6. aggregate refresh functions derive retained summary tables when the ingest path is `ingest_message(...)`

## Docker Container Overview

The default local stack runs as five cooperating containers:

- one publisher container
- one Mosquitto broker container
- two subscriber containers with different settings files and different ingest functions
- one TimescaleDB container

## Mermaid Class Diagram: Docker Containers

```mermaid
classDiagram
    class PublisherContainer {
        +image mqtt2postgres:local
        +module apps.publisher
        +mount /config/publisher-config.json
    }

    class BrokerContainer {
        +image eclipse-mosquitto:2
        +service mqtt-broker
        +port 1883
    }

    class SensorSubscriberContainer {
        +image mqtt2postgres:local
        +module mqtt2postgres
        +mount /config/subscriber-config.json
        +db_ingest_function mqtt_ingest.ingest_message
    }

    class TopicSubscriberContainer {
        +image mqtt2postgres:local
        +module mqtt2postgres
        +mount /config/subscriber-topics-config.json
        +db_ingest_function mqtt_ingest.ingest_topics
    }

    class TimescaleContainer {
        +image timescale/timescaledb-ha:pg16
        +service timescaledb
        +port 5432
    }

    class PublisherConfigFile {
        +publishers[]
        +topics[]
    }

    class SubscriberConfigFile {
        +topic_filters[]
        +db_ingest_function ingest_message
    }

    class TopicSubscriberConfigFile {
        +topic_filters # and $SYS/#
        +db_ingest_function ingest_topics
    }

    class SqlBootstrapDirectory {
        +schema
        +tables
        +functions
        +jobs
    }

    PublisherContainer "1" --> "1" PublisherConfigFile : loads
    SensorSubscriberContainer "1" --> "1" SubscriberConfigFile : loads
    TopicSubscriberContainer "1" --> "1" TopicSubscriberConfigFile : loads
    TimescaleContainer "1" --> "1" SqlBootstrapDirectory : initializes from

    PublisherContainer "1" --> "1" BrokerContainer : publishes to
    BrokerContainer "1" --> "1" SensorSubscriberContainer : delivers matching topics to
    BrokerContainer "1" --> "1" TopicSubscriberContainer : delivers matching topics to

    SensorSubscriberContainer "1" --> "1" TimescaleContainer : writes to
    TopicSubscriberContainer "1" --> "1" TimescaleContainer : writes to
```

## Container Responsibility Notes

- `mqtt-publisher` is stateless apart from its mounted publisher settings file.
- `mqtt-broker` is only the MQTT routing boundary; it does not persist the analytical model.
- `mqtt-subscriber` is the sensor-ingest process and is responsible for raw message persistence and aggregate refresh through `mqtt_ingest.ingest_message(...)`.
- `mqtt-subscriber-topics` is the topic-inventory process and is responsible for broker visibility through `mqtt_ingest.ingest_topics(...)`.
- `timescaledb` owns both raw persistence and derived aggregate tables.

## Mermaid Class Diagram

```mermaid
classDiagram
    class PublisherApp {
        +PublisherConfig settings
        +run_publishers()
        +publish_messages()
    }

    class PublisherSettingsFile {
        +publishers[]
        +topics[]
        +generator.kind
    }

    class Broker {
        +topics
        +publish()
        +distribute()
    }

    class SubscriberApp {
        +SubscriberSettings settings
        +start()
        +on_message()
    }

    class SubscriberSettingsFile {
        +mqtt_host
        +db_host
        +topic_filters[]
        +db_ingest_function
    }

    class IngestMessageFunction {
        +ingest_message(topic,payload,received_at,metadata)
    }

    class IngestTopicsFunction {
        +ingest_topics(topic,payload,received_at,metadata)
    }

    class MessagesTable {
        +topic
        +payload
        +numeric_value
        +device_id
        +metric_name
        +trace fields
    }

    class TopicOverviewTable {
        +topic
        +first_seen_at
        +last_seen_at
        +message_count
    }

    class Refresh3mFunction {
        +refresh_message_3m_aggregates()
    }

    class Refresh15mFunction {
        +refresh_message_15m_aggregates()
    }

    class Refresh60mFunction {
        +refresh_message_60m_aggregates()
    }

    class Refresh24hFunction {
        +refresh_message_24h_aggregates()
    }

    class Aggregate3mTable {
        +bucket_start
        +device_id
        +metric_name
        +quality_score
    }

    class Aggregate15mTable {
        +bucket_start
        +device_id
        +metric_name
        +quality_score
    }

    class Aggregate60mTable {
        +bucket_start
        +device_id
        +metric_name
        +quality_score
    }

    class Aggregate24hTable {
        +bucket_start
        +device_id
        +metric_name
        +quality_score
    }

    class PowerEnergyRecon3mTable {
        +bucket_start
        +device_id
        +power_locf_integral_ws
        +energy_locf_delta_ws
        +drift_locf_pct
    }

    class PowerEnergyRecon24hTable {
        +bucket_start
        +device_id
        +power_linear_integral_ws
        +energy_linear_delta_ws
        +drift_linear_pct
    }

    PublisherApp "1" --> "1" PublisherSettingsFile : loads
    PublisherApp "1..*" --> "1" Broker : publishes into

    SubscriberApp "1" --> "1" SubscriberSettingsFile : loads
    Broker "1" --> "0..*" SubscriberApp : distributes matching messages to

    SubscriberApp "0..*" --> "1" IngestMessageFunction : may call
    SubscriberApp "0..*" --> "1" IngestTopicsFunction : may call

    IngestMessageFunction "1" --> "0..*" MessagesTable : inserts rows into
    IngestTopicsFunction "1" --> "0..*" TopicOverviewTable : upserts rows into

    IngestMessageFunction "1" --> "1" Refresh3mFunction : triggers
    IngestMessageFunction "1" --> "1" Refresh15mFunction : triggers
    IngestMessageFunction "1" --> "1" Refresh60mFunction : triggers
    IngestMessageFunction "1" --> "1" Refresh24hFunction : triggers

    Refresh3mFunction "1" --> "0..*" MessagesTable : reads from
    Refresh15mFunction "1" --> "0..*" MessagesTable : reads from
    Refresh60mFunction "1" --> "0..*" MessagesTable : reads from
    Refresh24hFunction "1" --> "0..*" MessagesTable : reads from

    Refresh3mFunction "1" --> "0..*" Aggregate3mTable : writes
    Refresh15mFunction "1" --> "0..*" Aggregate15mTable : writes
    Refresh60mFunction "1" --> "0..*" Aggregate60mTable : writes
    Refresh24hFunction "1" --> "0..*" Aggregate24hTable : writes
    IngestMessageFunction "1" --> "1" PowerEnergyRecon3mTable : refreshes derived rows for
    IngestMessageFunction "1" --> "1" PowerEnergyRecon24hTable : refreshes derived rows for
```

## Cardinality Notes

- One publisher settings file can define many publishers.
- One publisher can publish many MQTT topics.
- One broker can distribute a single published message to zero, one, or many subscribers depending on topic matching.
- One subscriber instance is bound to one subscriber settings file at startup.
- One subscriber message callback results in exactly one database-function call for the configured ingest path.
- `mqtt_ingest.ingest_message(...)` inserts one raw row per routed message and may update multiple aggregate rows across multiple bucket widths.
- `mqtt_ingest.ingest_topics(...)` upserts one logical row per distinct topic in `mqtt_ingest.topic_overview`.

## Sequence Diagram: End-To-End Sensor Ingest

This is the normal path for sensor-like topics such as `sensors/node-1/temp`.

```mermaid
sequenceDiagram
    autonumber
    participant P as Python Publisher
    participant B as MQTT Broker
    participant S as Python Subscriber
    participant F as mqtt_ingest.ingest_message(...)
    participant M as mqtt_ingest.messages
    participant R3 as refresh_message_3m_aggregates(...)
    participant R15 as refresh_message_15m_aggregates(...)
    participant R60 as refresh_message_60m_aggregates(...)
    participant R24 as refresh_message_24h_aggregates(...)

    P->>B: PUBLISH topic + payload
    B-->>S: deliver matching MQTT message
    S->>S: match topic_filter and parse trace envelope
    S->>F: SELECT mqtt_ingest.ingest_message(...)
    F->>M: INSERT raw message row
    F->>R3: refresh touched 3m bucket
    F->>R15: refresh touched 15m bucket
    F->>R60: refresh touched 60m bucket
    F->>R24: refresh touched 24h bucket
    R3-->>F: bucket rows updated
    R15-->>F: bucket rows updated
    R60-->>F: bucket rows updated
    R24-->>F: bucket rows updated
    F-->>S: commit complete
```

## Sequence Diagram: Raw Data To Aggregate Tables

This sub-sequence focuses only on the database side after `mqtt_ingest.ingest_message(...)` has been called.

```mermaid
sequenceDiagram
    autonumber
    participant F as mqtt_ingest.ingest_message(...)
    participant M as mqtt_ingest.messages
    participant R3 as refresh_message_3m_aggregates(...)
    participant A3 as message_3m_aggregates
    participant R15 as refresh_message_15m_aggregates(...)
    participant A15 as message_15m_aggregates
    participant R60 as refresh_message_60m_aggregates(...)
    participant A60 as message_60m_aggregates
    participant R24 as refresh_message_24h_aggregates(...)
    participant A24 as message_24h_aggregates

    F->>M: insert topic, payload, numeric_value, metadata
    F->>R3: refresh affected 3m range
    R3->>M: read source rows and boundary context
    R3->>A3: upsert 3m aggregate rows

    F->>R15: refresh affected 15m range
    R15->>M: read source rows and boundary context
    R15->>A15: upsert 15m aggregate rows

    F->>R60: refresh affected 60m range
    R60->>M: read source rows and boundary context
    R60->>A60: upsert 60m aggregate rows

    F->>R24: refresh affected 24h range
    R24->>M: read source rows and boundary context
    R24->>A24: upsert 24h aggregate rows
```

## Sequence Diagram: Raw Data To Power/Energy Reconciliation Tables

```mermaid
sequenceDiagram
    autonumber
    participant F as mqtt_ingest.ingest_message(...)
    participant M as mqtt_ingest.messages
    participant R3 as refresh_power_energy_3m_reconciliation(...)
    participant PE3 as power_energy_3m_reconciliation
    participant R24 as refresh_power_energy_24h_reconciliation(...)
    participant PE24 as power_energy_24h_reconciliation

    F->>M: insert power or energy raw row
    F->>R3: refresh affected 3m reconciliation range
    R3->>M: read power and energy source rows plus boundary context
    R3->>PE3: upsert per-device reconciliation rows

    F->>R24: refresh affected 24h reconciliation range
    R24->>M: read power and energy source rows plus boundary context
    R24->>PE24: upsert per-device reconciliation rows
```

## Sequence Diagram: Topic Inventory Ingest

This is the parallel ingest path used for broker visibility.

```mermaid
sequenceDiagram
    autonumber
    participant P as Python Publisher or Broker Status Topic
    participant B as MQTT Broker
    participant S as Topic-Overview Subscriber
    participant F as mqtt_ingest.ingest_topics(...)
    participant T as mqtt_ingest.topic_overview

    P->>B: publish topic activity
    B-->>S: deliver matching topic
    S->>S: match broad filter (# or $SYS/#)
    S->>F: SELECT mqtt_ingest.ingest_topics(...)
    F->>T: INSERT or UPDATE distinct topic row
    F-->>S: commit complete
```

## Sequence Diagram: Topic Inventory Table Update

This sub-sequence isolates the database behavior of `mqtt_ingest.ingest_topics(...)`.

```mermaid
sequenceDiagram
    autonumber
    participant F as mqtt_ingest.ingest_topics(...)
    participant T as mqtt_ingest.topic_overview

    F->>T: look up or merge by full topic
    T-->>F: existing row or no row
    F->>T: write first_seen_at, last_seen_at, message_count, latest metadata
```

## Why The Two Ingest Functions Stay Separate

The split is intentional:

- `mqtt_ingest.ingest_message(...)` is optimized for retained event history and numeric aggregation
- `mqtt_ingest.ingest_topics(...)` is optimized for broker-topic discovery and last-seen state

If they were collapsed into a single ingest function, the runtime would mix two different persistence models:

- append-heavy time-series event storage
- per-topic upsert-style inventory tracking

That would make both the Python app configuration and the SQL behavior less explicit.

The power/energy reconciliation path is kept separate from the generic aggregate tables for the same reason. It is a metric-pair specific derivation with its own semantics:

- cumulative counter deltas on the `energy` side
- time integration on the `power` side
- explicit drift calculations between the two

## Reading The System From Left To Right

When explaining the system to another engineer, the cleanest order is:

1. publisher settings create one or more synthetic publisher clients
2. the MQTT broker is only the transport fan-out boundary
3. the subscriber chooses one ingest function per process
4. the database function owns the persistence semantics
5. aggregate tables are derived artifacts of `mqtt_ingest.messages`, not peer sources
