# AIE OKafka Consumer

Java consumer for the `AIE_EVENTS` TxEventQ OKafka topic.

## Runtime Credentials

The recommended entry point is the repository-level script:

```bash
cd ../..
./run_consumer.sh
```

That script prepares the wallet and local password file if they are missing.

If you run this client directly, the wallet must already be prepared under:

```text
../../wallet/tns_admin
```

The password is intentionally not stored in this project. Set it before running:

```bash
export OKAFKA_PASSWORD='<AIE-password>'
```

The username defaults to `AIE` from `config/consumer.properties`. Override it only if needed:

```bash
export OKAFKA_USER=AIE
```

## Direct Option 1: Maven exec

From this directory:

```bash
./bin/run-mvn-exec.sh
```

## Direct Option 2: Executable JAR

From this directory:

```bash
./bin/build-jar.sh
./bin/run-jar.sh
```

## End-to-End Test

Prepare the database side first from the repository root. If you are currently in this directory, run:

```bash
cd ../..
sql -S -L -name admin_adbdev2 @scripts/03_grant_okafka_client_prereqs.sql
sql -S -L -name AIE @scripts/00_reset_demo.sql
sql -S -L -name AIE @scripts/01_create_okafka_topic.sql
```

Start the consumer in one terminal:

```bash
./run_consumer.sh
```

Publish from another terminal at the repository root:

```bash
sql -S -L -name AIE @scripts/02_publish_messages.sql
```

The consumer should print the 10 JSON records published by the SQL script and keep waiting.
Stop the process with `Ctrl+C` when the demo is complete.

## Useful Overrides

Use a different consumer group to re-read messages from the beginning:

```bash
./bin/run-mvn-exec.sh --group-id=CGAIE$(date +%s)
```

Stop automatically after the demo batch:

```bash
./bin/run-mvn-exec.sh --max-messages=10
```

Enable an idle timeout only when you want the process to exit automatically while waiting:

```bash
./bin/run-mvn-exec.sh --idle-timeout-seconds=120
```

Use a different config file:

```bash
./bin/run-mvn-exec.sh --config=/path/to/consumer.properties
```
