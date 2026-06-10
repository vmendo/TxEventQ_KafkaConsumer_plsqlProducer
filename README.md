# TxEventQ Kafka Consumer with PL/SQL Producer

Demo assets for creating an Oracle TxEventQ topic that can be consumed with Oracle OKafka, publishing messages from PL/SQL, and running a Java Kafka-style consumer.

## What is Included

- `scripts/`: SQLcl scripts to grant OKafka prerequisites, reset the demo, create the OKafka-compatible topic, publish messages, and diagnose topic state.
- `client/okafka-consumer/`: Maven Java OKafka consumer with both `mvn exec:java` and executable JAR options.
- `doc/run_demo.md`: short step-by-step runbook.
- `doc/aie_txeventq_okafka_demo.md`: full English documentation.
- `doc/es/`: Spanish documentation.
- `history/bitacora.txt`: implementation log.

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
