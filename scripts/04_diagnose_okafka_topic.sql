-- Configure SQLcl output so diagnostic results are easy to read.
set serveroutput on
set verify off
set feedback on
set pagesize 200
set linesize 220

-- Keep running even if one optional diagnostic view is unavailable.
whenever sqlerror continue

prompt Diagnosing OKafka-compatible TxEventQ topic AIE_EVENTS

column user_name format a20
column service_name format a60

-- Confirm which schema and service are being inspected.
SELECT USER AS user_name,
       SYS_CONTEXT('USERENV', 'SERVICE_NAME') AS service_name
FROM   dual;

prompt Queue metadata
column name format a30
column queue_table format a30
column queue_type format a20
column enqueue_enabled format a16
column dequeue_enabled format a16
column sharded format a10

-- Verify that the topic exists, is sharded, and is enabled.
SELECT name,
       queue_table,
       queue_type,
       enqueue_enabled,
       dequeue_enabled,
       sharded
FROM   user_queues
WHERE  name = 'AIE_EVENTS';

prompt Queue table metadata
column type format a20
column user_comment format a80

-- Verify the payload type used by the OKafka topic.
SELECT queue_table,
       type,
       user_comment
FROM   user_queue_tables
WHERE  queue_table IN (
         SELECT queue_table
         FROM   user_queues
         WHERE  name = 'AIE_EVENTS'
       );

prompt Subscribers
column queue_name format a30
column consumer_name format a60
column address format a60

-- List durable subscribers created by OKafka consumer groups.
SELECT queue_name,
       consumer_name,
       address
FROM   user_queue_subscribers
WHERE  queue_name = 'AIE_EVENTS'
ORDER  BY consumer_name;

prompt Partition assignments
column owner format a20
column queue_name format a30
column subscriber_name format a60
column client_id format a30

-- Real assignments should show partition ids 0..N. A partition id of -1 is a placeholder.
SELECT *
FROM   user_queue_partition_assignment_table
WHERE  queue_name = 'AIE_EVENTS';

prompt Partition assignment summary

-- A placeholder assignment can be transient while OKafka rebalances. The consumer
-- is ready when real partition ids are present for its subscriber.
SELECT subscriber_name,
       SUM(CASE WHEN partition_id >= 0 THEN 1 ELSE 0 END) AS real_partition_count,
       SUM(CASE WHEN partition_id < 0 THEN 1 ELSE 0 END) AS placeholder_count
FROM   user_queue_partition_assignment_table
WHERE  queue_name = 'AIE_EVENTS'
GROUP  BY subscriber_name
ORDER  BY subscriber_name;

prompt Message counts by physical shard and state

-- State 0 means the message is ready in this queue table.
SELECT shard,
       state,
       COUNT(*) AS message_count
FROM   aie_events
GROUP  BY shard,
          state
ORDER  BY shard,
          state;

prompt Recent queue table rows
column msgid_hex format a40
column correlation format a30

-- Show the most recent physical rows in the queue table.
SELECT RAWTOHEX(msgid) AS msgid_hex,
       shard,
       seq_num,
       correlation,
       state,
       enqueue_time
FROM   (
         SELECT msgid,
                shard,
                seq_num,
                correlation,
                state,
                enqueue_time
         FROM   aie_events
         ORDER  BY enqueue_time DESC
       )
WHERE  ROWNUM <= 20;

prompt OKafka prerequisite visibility

-- These queries should return one row each when the OKafka prerequisites are in place.
SELECT 'GV$SESSION' AS object_name, COUNT(*) AS sample_count FROM sys.gv_$session WHERE ROWNUM <= 1;
SELECT 'V$SESSION' AS object_name, COUNT(*) AS sample_count FROM sys.v_$session WHERE ROWNUM <= 1;
SELECT 'GV$INSTANCE' AS object_name, COUNT(*) AS sample_count FROM sys.gv_$instance WHERE ROWNUM <= 1;
SELECT 'GV$LISTENER_NETWORK' AS object_name, COUNT(*) AS sample_count FROM sys.gv_$listener_network WHERE ROWNUM <= 1;
SELECT 'GV$PDBS' AS object_name, COUNT(*) AS sample_count FROM sys.gv_$pdbs WHERE ROWNUM <= 1;

prompt Optional direct AQ subscriber browse test

-- Try to browse one message as the newest OKafka subscriber. This does not remove
-- or lock messages permanently. This uses DBMS_AQ directly, so treat failures as
-- advisory for troubleshooting; OKafka may still rebalance and consume normally.
DECLARE
  l_subscriber USER_QUEUE_PARTITION_ASSIGNMENT_TABLE.SUBSCRIBER_NAME%TYPE;
  l_deq_opts   DBMS_AQ.DEQUEUE_OPTIONS_T;
  l_msg_props  DBMS_AQ.MESSAGE_PROPERTIES_T;
  l_payload    SYS.AQ$_JMS_BYTES_MESSAGE;
  l_msgid      RAW(16);
  l_raw        RAW(32767);
BEGIN
  SELECT subscriber_name
  INTO   l_subscriber
  FROM   (
           SELECT subscriber_name,
                  create_time
           FROM   user_queue_partition_assignment_table
           WHERE  queue_name = 'AIE_EVENTS'
           ORDER  BY create_time DESC
         )
  WHERE  ROWNUM = 1;

  DBMS_OUTPUT.PUT_LINE('Browsing as subscriber: ' || l_subscriber);

  l_deq_opts.consumer_name := l_subscriber;
  l_deq_opts.dequeue_mode  := DBMS_AQ.BROWSE;
  l_deq_opts.navigation    := DBMS_AQ.FIRST_MESSAGE;
  l_deq_opts.wait          := 1;
  l_deq_opts.visibility    := DBMS_AQ.IMMEDIATE;

  DBMS_AQ.DEQUEUE(
    queue_name         => 'AIE_EVENTS',
    dequeue_options    => l_deq_opts,
    message_properties => l_msg_props,
    payload            => l_payload,
    msgid              => l_msgid
  );

  l_payload.GET_BYTES(l_raw);
  DBMS_OUTPUT.PUT_LINE('Browse OK. msgid=' || RAWTOHEX(l_msgid));
  DBMS_OUTPUT.PUT_LINE('Payload preview=' || SUBSTR(UTL_RAW.CAST_TO_VARCHAR2(l_raw), 1, 300));
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    DBMS_OUTPUT.PUT_LINE('Browse skipped: no OKafka subscriber assignment exists yet.');
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Browse failed: ' || SQLCODE || ' ' || SQLERRM);
    IF SQLCODE = -24003 THEN
      DBMS_OUTPUT.PUT_LINE('Note: ORA-24003 can appear for a direct DBMS_AQ browse while OKafka is still able to rebalance and consume.');
      DBMS_OUTPUT.PUT_LINE('Action: use the partition assignment summary and the Java consumer log as the source of truth.');
    END IF;
END;
/
