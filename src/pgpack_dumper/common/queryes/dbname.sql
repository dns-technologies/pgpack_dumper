select case when count(nspname) = 1 then 'greenplum' else 'postgres' end as dbname,
pg_is_in_recovery() is_readonly from pg_catalog.pg_namespace where nspname = 'gp_toolkit';