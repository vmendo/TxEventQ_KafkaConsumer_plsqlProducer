set serveroutput on
set verify off
set feedback on
whenever sqlerror exit sql.sqlcode

prompt Granting Oracle OKafka client prerequisites to AIE

-- This script must be executed as ADMIN or as a privileged user.
-- Oracle OKafka consumers need these privileges to discover sessions,
-- instances, listener endpoints, PDB metadata, and TxEventQ partition assignments.
-- The privilege list follows the oracle/okafka project README for Autonomous Database.

grant AQ_USER_ROLE to AIE;
grant execute on DBMS_AQ to AIE;
grant execute on DBMS_AQADM to AIE;
grant execute on DBMS_AQIN to AIE;
grant execute on DBMS_TEQK to AIE;

grant select on SYS.GV_$SESSION to AIE;
grant select on SYS.V_$SESSION to AIE;
grant select on SYS.GV_$INSTANCE to AIE;
grant select on SYS.GV_$LISTENER_NETWORK to AIE;
grant select on SYS.GV_$PDBS to AIE;
grant select on SYS.DBA_RSRC_PLAN_DIRECTIVES to AIE;

-- Oracle documents this grant as an OKafka prerequisite. In Autonomous Database,
-- ADMIN may not be allowed to grant SYS.USER_QUEUE_PARTITION_ASSIGNMENT_TABLE
-- directly. AIE can still query its USER_QUEUE_PARTITION_ASSIGNMENT_TABLE view.
declare
  l_sqlcode number;
begin
  execute immediate 'grant select on SYS.USER_QUEUE_PARTITION_ASSIGNMENT_TABLE to AIE';
  dbms_output.put_line('Granted SYS.USER_QUEUE_PARTITION_ASSIGNMENT_TABLE to AIE.');
exception
  when others then
    l_sqlcode := sqlcode;
    if l_sqlcode in (-942, -1031) then
      dbms_output.put_line('Warning: could not grant SYS.USER_QUEUE_PARTITION_ASSIGNMENT_TABLE directly: ' || sqlerrm);
      dbms_output.put_line('AIE must be able to query USER_QUEUE_PARTITION_ASSIGNMENT_TABLE in its own schema.');
    else
      raise;
    end if;
end;
/

begin
  -- Grant Resource Manager privileges required by the OKafka partition assignment logic.
  DBMS_AQADM.GRANT_PRIV_FOR_RM_PLAN('AIE');
end;
/

prompt OKafka client prerequisites granted to AIE
