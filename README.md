# TxEventQ Kafka Consumer with PL/SQL Producer

Demo assets for creating an Oracle TxEventQ topic that can be consumed with Oracle OKafka, publishing messages from PL/SQL, and running a Java Kafka-style consumer.

## Demo Video

Watch [TxEventQ.mov](doc/TxEventQ.mov) for a short two-terminal run: one terminal starts the OKafka consumer, and the other publishes messages with SQLcl so the consumed records are visible immediately.

## Intro

This demo does one specific thing: it creates an Oracle TxEventQ topic that OKafka consumers can read, publishes sample JSON messages with a PL/SQL script executed from SQLcl, and shows a Java Kafka-style consumer receiving those messages.

It does not create business tables, detect table changes, define triggers, or implement Change Data Capture. Those patterns can be built around TxEventQ, but they are intentionally outside the scope of this repository.

Where this pattern can fit:

- Publishing explicit business events from PL/SQL, such as `ORDER_ACCEPTED`, `PAYMENT_APPROVED`, or `BATCH_COMPLETED`.
- Notifying remote services from stored procedures, scheduled jobs, or application-controlled PL/SQL without polling tables.
- Giving remote Java applications a Kafka-style consumer API while keeping the event stream inside Oracle Database.

For full Change Data Capture, replication, heterogeneous synchronization, or log-based data movement across systems, Oracle GoldenGate is the better architectural fit. Oracle [describes GoldenGate](https://docs.oracle.com/en/database/goldengate/core/26/) as supporting high availability, real-time data integration, transactional change data capture, data replication, transformations, and verification between operational and analytical systems.

## What is Included

- `scripts/`: SQLcl scripts to grant OKafka prerequisites, reset the demo, create the OKafka-compatible topic, publish messages, and diagnose topic state.
- `client/okafka-consumer/`: Maven Java OKafka consumer with both `mvn exec:java` and executable JAR options.
- `doc/run_demo.md`: short step-by-step runbook.
- `doc/aie_txeventq_okafka_demo.md`: full English documentation.
- `doc/es/`: Spanish documentation.

## What is Not Included

Runtime secrets and local environment files are intentionally excluded:

- ADB wallet ZIP files.
- Extracted wallet files under `wallet/tns_admin`.
- `.pwd.txt`.
- Maven build output under `client/okafka-consumer/target`.

## Quick Start

Read the short runbook first:

```text
doc/run_demo.md
```

The usual flow is:

```bash
sql -S -L -name admin_adbdev2 @scripts/03_grant_okafka_client_prereqs.sql
sql -S -L -name AIE @scripts/00_reset_demo.sql
sql -S -L -name AIE @scripts/01_create_okafka_topic.sql
./run_consumer.sh
sql -S -L -name AIE @scripts/02_publish_messages.sql
```

On the first consumer run, `run_consumer.sh` asks for the wallet path and database password if they are not already prepared locally.
