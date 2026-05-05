#!/bin/bash

set -ev

sqlplus system/${DATABASE_SYS_PASSWORD}@${DATABASE_NAME} <<SQL
@@spec/support/unlock_and_setup_hr_user.sql
@@spec/support/create_arunit_user.sql
exit
SQL

# `grant execute on dbms_lock` requires SYS in 12c+; SYSTEM no longer has the
# privilege. Run it through a separate `as sysdba` session so the same script
# works on both gvenzl/oracle-xe:11 and gvenzl/oracle-free.
sqlplus -s sys/${DATABASE_SYS_PASSWORD}@${DATABASE_NAME} as sysdba <<SQL
whenever sqlerror exit failure
grant execute on dbms_lock to hr;
exit
SQL
