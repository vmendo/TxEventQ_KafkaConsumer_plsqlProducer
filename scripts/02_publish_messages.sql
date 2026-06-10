-- Configure SQLcl output so DBMS_OUTPUT messages and query feedback are visible.
set serveroutput on
set verify off
set feedback on
set pagesize 100
set linesize 200

-- Stop execution immediately if any SQL or PL/SQL error occurs.
whenever sqlerror exit sql.sqlcode

prompt Publishing sample JSON messages to OKafka-compatible TxEventQ topic AIE_EVENTS

DECLARE
  -- Topic and publish settings for this demo run.
  c_topic_name        CONSTANT VARCHAR2(128) := 'AIE_EVENTS';
  c_message_count     CONSTANT PLS_INTEGER   := 10;
  -- Valid SHARDID values for this topic were tested as 0 through 5.
  c_shard_count       CONSTANT PLS_INTEGER   := 6;

  -- Variables used to validate the target topic before publishing.
  l_topic_exists      NUMBER;
  l_queue_table       USER_QUEUES.QUEUE_TABLE%TYPE;
  l_payload_type      USER_QUEUE_TABLES.TYPE%TYPE;

  -- AQ enqueue state and JMS bytes payload objects.
  l_enqueue_options   DBMS_AQ.ENQUEUE_OPTIONS_T;
  l_message_props     DBMS_AQ.MESSAGE_PROPERTIES_T;
  l_payload           SYS.AQ$_JMS_BYTES_MESSAGE;
  l_msgid             RAW(16);

  -- Event fields used to build the sample JSON payload.
  l_event_key         VARCHAR2(64);
  l_event_id          VARCHAR2(64);
  l_event_json        VARCHAR2(32767);
  l_created_at_utc    VARCHAR2(64);
  l_shard_id          PLS_INTEGER;
BEGIN
  -- Ensure the OKafka topic exists before attempting to publish messages.
  SELECT COUNT(*)
  INTO   l_topic_exists
  FROM   user_queues
  WHERE  name = c_topic_name;

  IF l_topic_exists = 0 THEN
    RAISE_APPLICATION_ERROR(
      -20001,
      'Topic ' || c_topic_name || ' does not exist. Run scripts/01_create_okafka_topic.sql first.'
    );
  END IF;

  -- Read the queue table metadata and confirm that this topic uses JMS_BYTES.
  -- The publisher uses SYS.AQ$_JMS_BYTES_MESSAGE, so this payload type is required.
  SELECT q.queue_table,
         qt.type
  INTO   l_queue_table,
         l_payload_type
  FROM   user_queues q
         JOIN user_queue_tables qt
           ON qt.queue_table = q.queue_table
  WHERE  q.name = c_topic_name;

  IF l_payload_type <> 'JMS_BYTES' THEN
    RAISE_APPLICATION_ERROR(
      -20002,
      'Topic ' || c_topic_name || ' uses payload type ' || l_payload_type ||
      '; this publisher expects JMS_BYTES.'
    );
  END IF;

  -- Make each enqueue part of the current transaction and store messages durably.
  l_enqueue_options.visibility    := DBMS_AQ.ON_COMMIT;
  l_enqueue_options.delivery_mode := DBMS_AQ.PERSISTENT;

  FOR i IN 1..c_message_count LOOP
    -- Build a deterministic logical key plus a unique event id for each message.
    l_event_key      := 'AIE_KEY_' || LPAD(i, 4, '0');
    l_event_id       := 'AIE_EVT_' || LOWER(RAWTOHEX(SYS_GUID()));
    -- Assign the message to an explicit TxEventQ shard to avoid ORA-25600.
    l_shard_id       := MOD(i - 1, c_shard_count);
    -- Store timestamps in UTC so consumers can compare events consistently.
    l_created_at_utc := TO_CHAR(
                          SYSTIMESTAMP AT TIME ZONE 'UTC',
                          'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'
                        );

    -- Build a compact JSON payload that can be read as a String by OKafka.
    l_event_json :=
      '{' ||
      '"eventId":"' || l_event_id || '",' ||
      '"eventType":"ORDER_CREATED",' ||
      '"source":"PLSQL_DBMS_AQ",' ||
      '"topic":"' || c_topic_name || '",' ||
      '"eventKey":"' || l_event_key || '",' ||
      '"oracleShardId":' || TO_CHAR(l_shard_id) || ',' ||
      '"sequence":' || TO_CHAR(i) || ',' ||
      '"customerId":"CUST-' || LPAD(MOD(i, 5) + 1, 4, '0') || '",' ||
      '"orderId":"ORD-' || LPAD(i, 6, '0') || '",' ||
      '"amount":' || TO_CHAR(100 + i * 12.75, 'FM9999990D00', 'NLS_NUMERIC_CHARACTERS=.,') || ',' ||
      '"currency":"EUR",' ||
      '"createdAt":"' || l_created_at_utc || '"' ||
      '}';

    -- Create a JMS bytes message and store the JSON payload as raw bytes.
    l_payload := SYS.AQ$_JMS_BYTES_MESSAGE.CONSTRUCT;
    l_payload.SET_BYTES(UTL_RAW.CAST_TO_RAW(l_event_json));

    -- Add JMS properties that consumers can use for filtering or diagnostics.
    l_payload.SET_STRING_PROPERTY('content_type', 'application/json');
    l_payload.SET_STRING_PROPERTY('source', 'plsql');
    l_payload.SET_STRING_PROPERTY('event_type', 'ORDER_CREATED');
    l_payload.SET_STRING_PROPERTY('event_key', l_event_key);

    -- Set AQ message properties, including correlation and the explicit shard.
    l_message_props := DBMS_AQ.MESSAGE_PROPERTIES_T();
    l_message_props.correlation := l_event_key;
    l_message_props.expiration  := DBMS_AQ.NEVER;
    l_message_props.shardid     := l_shard_id;

    -- Enqueue the message into the OKafka-compatible TxEventQ topic.
    DBMS_AQ.ENQUEUE(
      queue_name         => c_topic_name,
      enqueue_options    => l_enqueue_options,
      message_properties => l_message_props,
      payload            => l_payload,
      msgid              => l_msgid
    );

    -- Print the generated message id and payload for demo traceability.
    DBMS_OUTPUT.PUT_LINE(
      'Enqueued ' || l_event_key ||
      ' shardid=' || l_shard_id ||
      ' msgid=' || RAWTOHEX(l_msgid) ||
      ' payload=' || l_event_json
    );
  END LOOP;

  -- Commit once after all messages are enqueued so they become visible to consumers.
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Committed ' || c_message_count || ' messages to ' || c_topic_name || '.');
END;
/
