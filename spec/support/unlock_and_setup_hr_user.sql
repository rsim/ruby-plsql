create user hr identified by hr;
alter user hr identified by hr account unlock;
grant dba to hr;
grant execute on dbms_lock to hr;
