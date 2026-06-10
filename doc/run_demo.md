# Run the AIE TxEventQ OKafka Demo

Simple runbook to execute the demo end to end.

Watch [TxEventQ.mp4](TxEventQ.mp4) first if you want to see the intended two-terminal flow: start the consumer, publish messages from SQLcl, and watch the records arrive.

Run the commands from the repository root unless a step explicitly changes directory.

## 1. Prepare the Database

Run the OKafka prerequisites as `ADMIN`:

```bash
sql -S -L -name admin_adbdev2 @scripts/03_grant_okafka_client_prereqs.sql
```

Reset and recreate the OKafka topic as `AIE`:

```bash
sql -S -L -name AIE @scripts/00_reset_demo.sql
sql -S -L -name AIE @scripts/01_create_okafka_topic.sql
```

## 2. Start the Consumer

Open terminal 1 from the repository root:

```bash
./run_consumer.sh
```

On the first run, the script asks for anything missing:

- ADB wallet ZIP or extracted wallet directory, if `wallet/tns_admin` does not exist.
- AIE database password, if `.pwd.txt` does not exist.

The password is saved locally in `.pwd.txt` with owner-only permissions. The wallet and `.pwd.txt` are ignored by Git.

By default, the consumer keeps waiting for records and does not exit automatically. Leave it running while you publish messages, then stop it with `Ctrl+C` when the demo is complete.

Optional consumer arguments can be passed through:

```bash
./run_consumer.sh --max-messages=10
./run_consumer.sh --idle-timeout-seconds=120
./run_consumer.sh --group-id=CGAIE$(date +%s)
```

## 3. Publish Messages

Open terminal 2 from the repository root:

```bash
sql -S -L -name AIE @scripts/02_publish_messages.sql
```

## 4. Expected Result

The consumer should print 10 JSON messages from `AIE_EVENTS`. Because the default mode is interactive, stop it with `Ctrl+C` after reviewing the output.

If you run it with `--max-messages=10`, it finishes with:

```text
Committed batch. Total consumed: 10
Consumer finished. Total consumed: 10
```

## 5. Notes

- Run the Java consumer from a shell with network access to the ADB host.
- If the consumer initially shows `AIE_EVENTS--1`, keep it running for a few minutes. OKafka may later revoke the placeholder and assign real partitions.
- Use `sql -S -L -name AIE @scripts/04_diagnose_okafka_topic.sql` to inspect subscribers, partition assignments, and queued messages.
- A fresh `group.id` with `auto.offset.reset=earliest` reads retained backlog. For a clean 10-message demo, reset/recreate the topic before starting the consumer.
- The diagnostic script is read-only and does not call direct AQ browse/dequeue.
- The wallet is prepared under `wallet/tns_admin`.
