-- Configure SQLcl output so the reset process is visible and repeatable.
set serveroutput on
set verify off
set feedback on
set pagesize 100
set linesize 200

-- Stop execution immediately if any SQL or PL/SQL error occurs.
whenever sqlerror exit sql.sqlcode

prompt Resetting AIE TxEventQ/OKafka demo

DECLARE
  l_topic_exists NUMBER;
BEGIN
  -- Check whether the OKafka-compatible topic currently exists in this schema.
  SELECT COUNT(*)
  INTO   l_topic_exists
  FROM   user_queues
  WHERE  name = 'AIE_EVENTS';

  IF l_topic_exists > 0 THEN
    -- Drop the OKafka topic and its underlying TxEventQ objects.
    -- This leaves the demo ready to be created again from a clean state.
    DBMS_AQADM.DROP_DATABASE_KAFKA_TOPIC(
      topicname => 'AIE_EVENTS'
    );

    DBMS_OUTPUT.PUT_LINE('Dropped OKafka topic AIE_EVENTS.');
  ELSE
    -- Keep the reset script idempotent: do nothing if the topic is already absent.
    DBMS_OUTPUT.PUT_LINE('OKafka topic AIE_EVENTS does not exist. Nothing to reset.');
  END IF;
END;
/

prompt Remaining queue metadata for AIE_EVENTS
-- Show any remaining queue metadata. A clean reset should return no rows.
SELECT name,
       queue_table,
       queue_type,
       enqueue_enabled,
       dequeue_enabled,
       sharded
FROM   user_queues
WHERE  name = 'AIE_EVENTS';
