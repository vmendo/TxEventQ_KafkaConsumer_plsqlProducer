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
SELECT 'DBA_RSRC_PLAN_DIRECTIVES' AS object_name, COUNT(*) AS sample_count FROM sys.dba_rsrc_plan_directives WHERE ROWNUM <= 1;

prompt Diagnostic complete

-- This script intentionally does not call DBMS_AQ.DEQUEUE/BROWSE. During testing,
-- direct AQ browse calls were observed to coincide with OKafka rebalance events,
-- so this diagnostic remains read-only and metadata-focused.
