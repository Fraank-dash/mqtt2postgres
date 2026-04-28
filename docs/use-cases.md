# Use Cases

This document describes the most common ways to use the repo and how the main runtime pieces fit each scenario.

## Use Case 1: Simulate A Small Sensor Fleet

Goal:

- generate repeatable MQTT traffic for topics like `sensors/<device>/<metric>`
- vary per-topic ranges and generator shapes
- feed a local broker and database stack without writing custom publisher code

Relevant pieces:

- `examples/local-stack/publisher-config.json`
- `apps.publisher`
- `mqtt-broker`

Typical flow:

1. define one or more publishers in a JSON or YAML settings file
2. define topics and generators for each publisher
3. run `python -m apps.publisher --config ...` or start the Compose stack
4. inspect broker, subscriber, and database logs

Best fit when:

- you want a local data simulator
- you want stable random sequences via per-topic seeds
- you want multiple topics to move independently inside one publish cycle

## Use Case 2: Ingest Sensor Topics Into Raw And Aggregate Tables

Goal:

- persist MQTT payloads into `mqtt_ingest.messages`
- derive `3m`, `15m`, `60m`, and `24h` aggregate rows
- analyze device and metric behavior after the raw messages arrive

Relevant pieces:

- `examples/local-stack/subscriber-config.json`
- `apps.subscriber`
- `mqtt_ingest.ingest_message(...)`

Typical flow:

1. configure focused topic filters such as `sensors/+/temp`
2. start the subscriber
3. publish MQTT payloads
4. verify raw rows in `mqtt_ingest.messages`
5. verify derived rows in the aggregate tables

Best fit when:

- you care about time-series retention and analytics
- you want boundary-aware aggregate refresh
- you want trace metadata preserved from publish through commit

## Use Case 3: Build A Broker-Wide Topic Inventory

Goal:

- record which MQTT topics are active
- keep last-seen state for application topics and broker status topics
- observe broker coverage without treating the topic inventory as a time-series aggregate

Relevant pieces:

- `examples/local-stack/subscriber-topics-config.json`
- `mqtt_ingest.ingest_topics(...)`
- `mqtt_ingest.topic_overview`

Typical flow:

1. configure broad topic filters such as `#` and `$SYS/#`
2. start the topic-overview subscriber
3. let publishers and broker status messages flow
4. inspect `mqtt_ingest.topic_overview`

Best fit when:

- you want operational visibility
- you need topic discovery
- you want last-seen and count semantics rather than bucket aggregates

## Use Case 4: Learn A Simulated Twin From Retained Aggregates

Goal:

- derive realistic publisher settings from retained aggregate rows
- turn observed topic behavior back into synthetic publishers
- close the loop between ingestion and simulation

Relevant pieces:

- `mqtt2postgres-twin-config`
- `apps.publisher.twin_config`
- `mqtt_ingest.message_24h_aggregates`

Typical flow:

1. collect enough retained aggregate data
2. run `mqtt2postgres-twin-config`
3. inspect the generated publisher settings document
4. run `apps.publisher` with the generated output

Best fit when:

- you want a lightweight digital-twin workflow
- you want to reuse observed cadences and value ranges
- you want `clipped_normal` generators inferred from retained statistics

## Use Case 5: Debug End-To-End Timing

Goal:

- compare publish time, subscriber receive time, and database commit time
- confirm that one logical event stayed correlated across the runtime chain

Relevant pieces:

- traced JSON payloads from `apps.publisher`
- `observability.tracing`
- `mqtt_ingest.messages`

Typical flow:

1. publish traced JSON payloads
2. inspect subscriber logs
3. query raw message rows and trace fields
4. compare `published_at`, `received_at`, and commit timing

Best fit when:

- you are debugging lag or routing behavior
- you want one event lineage through the system
- you need to explain where timing was introduced

## Use Case 6: Explain Aggregate Quality To Another Engineer

Goal:

- show how raw sensor rows become retained quality-rated aggregate rows
- explain why a bucket is strong, weak, provisional, or incomplete

Relevant pieces:

- `mqtt_ingest.messages`
- `mqtt_ingest.message_*_aggregates`
- `docs/aggregate-status-and-quality.md`
- `docs/system-architecture.md`

Typical flow:

1. start at one raw message topic
2. identify the aggregate bucket widths that consume it
3. inspect boundary fields, count, interval spacing, and trust metrics
4. explain `status`, `quality_status`, `quality_score`, and `quality_flags`

Best fit when:

- you need analytical explainability
- you need to justify aggregate usefulness after raw pruning
- you want a stable teaching path for the retained-data model

## Use Case 7: Reconcile Power Against Cumulative Energy

Goal:

- track per-bucket energy from `sensors/<device>/energy`
- track the same bucket from integrated `sensors/<device>/power`
- detect drift when the cumulative counter and the integrated power no longer agree

Relevant pieces:

- `mqtt_ingest.power_energy_*_reconciliation`
- `mqtt_ingest.refresh_power_energy_*_reconciliation(...)`
- `docs/system-architecture.md`

Typical flow:

1. ingest both `sensors/<device>/power` and `sensors/<device>/energy`
2. inspect reconciliation rows for the target device and bucket width
3. compare:
   - `power_locf_integral_ws` vs `energy_locf_delta_ws`
   - `power_linear_integral_ws` vs `energy_linear_delta_ws`
4. use signed, absolute, and percent drift fields to decide whether the integration method needs adjustment

Best fit when:

- `energy` is rounded and you still want to track the counter directly
- `power` should provide a more continuous estimate across the bucket
- you want both methods persisted side by side so drift stays inspectable over time
