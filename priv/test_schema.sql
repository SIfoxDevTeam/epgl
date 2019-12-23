CREATE USER epgl_test WITH REPLICATION SUPERUSER PASSWORD 'epgl_test'; -- we need SUPERUSER to create publications

SELECT pg_drop_replication_slot('epgl_test_repl_slot');
SELECT pg_terminate_backend(pid) FROM pg_stat_activity where datname='epgl_test_db';
DROP DATABASE epgl_test_db;

CREATE DATABASE epgl_test_db WITH ENCODING 'UTF8';

GRANT ALL ON DATABASE epgl_test_db to epgl_test;

\c epgl_test_db;

CREATE EXTENSION pglogical;

SELECT pglogical.create_node(
    node_name := 'epgl_provider',
    dsn := 'host=localhost port=10432 dbname=epgl_test_db'
);

CREATE TABLE test_table1 (id integer primary key, value text);
CREATE TABLE test_table2 (id integer primary key, value text);
CREATE TABLE test_table4 (id integer primary key, value text);


INSERT INTO test_table1 (id, value) VALUES (1, 'one');
INSERT INTO test_table1 (id, value) VALUES (2, 'two');
INSERT INTO test_table1 (id) VALUES (3);

INSERT INTO test_table2 (id, value) VALUES (1, 'one');
INSERT INTO test_table2 (id, value) VALUES (2, 'two');

CREATE TABLE test_table3 (
  id integer primary key,
  c_bool bool,
  c_char char,
  c_int2 int2,
  c_int4 int4,
  c_int8 int8,
  c_float4 float4,
  c_float8 float8,
  c_bytea bytea,
  c_text text,
  c_varchar varchar(64),
  c_date date,
  c_time time,
  c_timetz timetz,
  c_timestamp timestamp,
  c_timestamptz timestamptz,
  c_interval interval,
  c_cidr cidr,
  c_inet inet,
  c_jsonb jsonb);

GRANT ALL ON TABLE test_table1 TO epgl_test;
GRANT ALL ON TABLE test_table2 TO epgl_test;
GRANT ALL ON TABLE test_table3 TO epgl_test;
GRANT ALL ON TABLE test_table4 TO epgl_test;

GRANT USAGE ON SCHEMA pglogical TO epgl_test;
GRANT SELECT ON pglogical.tables TO epgl_test;

SELECT pglogical.create_replication_set('epgl_test_repl_set_1', true, true, true);
SELECT pglogical.replication_set_add_table('epgl_test_repl_set_1', 'test_table1', false);
SELECT pglogical.replication_set_add_table('epgl_test_repl_set_1', 'test_table3', false);
SELECT pglogical.replication_set_add_table('epgl_test_repl_set_1', 'test_table4', false);

DO $$
DECLARE v_version int;
BEGIN
  SELECT current_setting('server_version_num')::int INTO v_version;
  IF v_version >= 100000 THEN
    EXECUTE 'CREATE PUBLICATION epgl_test_repl_set_1 FOR TABLE test_table1, test_table3, test_table4';
  END IF;
END $$;
