-- Some helper functions
CREATE OR REPLACE FUNCTION get_epoch(ts timestamptz) RETURNS float AS
$BODY$
    SELECT extract(epoch from ts);
$BODY$
LANGUAGE sql;

CREATE OR REPLACE PROCEDURE clean_spans() AS $$
BEGIN
    PERFORM count(*) from pg_tracing_consume_spans;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE reset_settings()
LANGUAGE SQL
AS $$
    SET pg_tracing.filter_query_ids TO DEFAULT;
    SET pg_tracing.sample_rate TO DEFAULT;
    SET pg_tracing.caller_sample_rate TO DEFAULT;
    SET pg_tracing.track_utility TO DEFAULT;
    SET pg_tracing.max_parameter_size TO DEFAULT;
    SET parallel_setup_cost TO DEFAULT;
    SET parallel_tuple_cost TO DEFAULT;
    SET min_parallel_table_scan_size TO DEFAULT;
    SET max_parallel_workers_per_gather TO DEFAULT;
$$;

CREATE OR REPLACE PROCEDURE reset_pg_tracing_test_table() AS $$
BEGIN
    DROP TABLE IF EXISTS pg_tracing_test;
    CREATE TABLE pg_tracing_test (a int, b char(20));
    COMMIT;
    CREATE INDEX pg_tracing_index ON pg_tracing_test (a);
    INSERT INTO pg_tracing_test VALUES(generate_series(1, 10000), 'aaa');
    ANALYZE pg_tracing_test;
END;
$$ LANGUAGE plpgsql;

-- Create test tables with data
CALL reset_pg_tracing_test_table();

-- Create test table to test modifications
CREATE TABLE test_modifications (a int, b char(20));

CREATE TABLE m AS SELECT i AS k, (i || ' v')::text v FROM generate_series(1, 16, 3) i;
ALTER TABLE m ADD UNIQUE (k);
