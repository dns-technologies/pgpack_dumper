select json_agg(column_info)::text::bytea as metadata from (select json_build_object(attname, json_build_object(
'oid', atttypid::int4, 'length', case when atttypid = 1700 then case when (atttypmod - 4) >> 16 = -1 then 10 else
(atttypmod - 4) >> 16 end when atttypmod <> -1 then atttypmod - 4 else attlen end, 'scale', case when atttypid = 1700
then case when (atttypmod - 4) >> 16 = -1 then 0 else (atttypmod - 4) & 65535 end else 0 end, 'nested', attndims)) as column_info
from pg_attribute where attrelid = '{table_name}'::regclass and attnum > 0 and not attisdropped order by attnum) as columns;