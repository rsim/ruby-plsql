#!/bin/bash

set -ev

"$ORACLE_HOME/bin/sqlplus" -L -S / AS SYSDBA <<SQL
@@spec/support/unlock_and_setup_hr_user.sql
@@spec/support/create_arunit_user.sql
exit
SQL
