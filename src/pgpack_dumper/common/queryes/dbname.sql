select case when count(nspname) = 1 then 'greenplum' else 'postgres' end as dbname
from pg_catalog.pg_namespace where nspname = 'gp_toolkit';