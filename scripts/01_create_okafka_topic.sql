-- Configure SQLcl output so each step is visible when the script is run.
set serveroutput on
set verify off
set feedback on
set pagesize 100
set linesize 200

-- Stop execution immediately if any SQL or PL/SQL error occurs.
whenever sqlerror exit sql.sqlcode

prompt Creating OKafka-compatible TxEventQ topic AIE_EVENTS

DECLARE
  l_topic_exists NUMBER;
BEGIN
  -- Check if the topic already exists so the script can be rerun safely.
  SELECT COUNT(*)
  INTO   l_topic_exists
  FROM   user_queues
  WHERE  name = 'AIE_EVENTS';

  IF l_topic_exists = 0 THEN
    -- Create an OKafka topic backed by a multi-consumer TxEventQ.
    -- Kafka consumers must consume a topic created by KafkaAdmin or by
    -- DBMS_AQADM.CREATE_DATABASE_KAFKA_TOPIC.
    DBMS_AQADM.CREATE_DATABASE_KAFKA_TOPIC(
      topicname                 => 'AIE_EVENTS',
      partition_num             => 3,
      retentiontime             => 7 * 24 * 3600,
      -- Required for OKafka topics created through DBMS_AQADM.
      partition_assignment_mode => 2,
      -- No topic replication is configured for this local demo.
      replication_mode          => DBMS_AQADM.NONE
    );

    DBMS_OUTPUT.PUT_LINE('Created OKafka topic AIE_EVENTS with 3 partitions and 7 day retention.');
  ELSE
    -- Keep the creation script idempotent: do nothing if the topic exists.
    DBMS_OUTPUT.PUT_LINE('OKafka topic AIE_EVENTS already exists. No changes applied.');
  END IF;
END;
/

-- Format the verification query output for SQLcl.
column name format a30
column queue_table format a30
column queue_type format a20
column enqueue_enabled format a16
column dequeue_enabled format a16
column sharded format a10

prompt Queue metadata
-- Verify the queue exists and is enabled for enqueue/dequeue operations.
SELECT name,
       queue_table,
       queue_type,
       enqueue_enabled,
       dequeue_enabled,
       sharded
FROM   user_queues
WHERE  name = 'AIE_EVENTS';

prompt Queue table metadata
-- Verify the queue table payload type. OKafka topics created here use JMS_BYTES.
SELECT queue_table,
       type,
       user_comment
FROM   user_queue_tables
WHERE  queue_table IN (
         SELECT queue_table
         FROM   user_queues
         WHERE  name = 'AIE_EVENTS'
       );
