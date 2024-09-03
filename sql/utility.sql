-- Create pg_tracing extension with sampling on
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ CREATE EXTENSION pg_tracing;

-- This will create the following spans (non exhaustive list):
--
-- +-------------------------------------------------------------------------------------+
-- | A: Utility: Create Extension                                                        |
-- +-+-----------------------------------------------------------------------------------+
--   +----------------------------------------------------------------------------------+
--   |B: ProcessUtility: Create Extension                                               |
--   +---+-----------------------------------+---+--------------------------------------+
--       +-----------------------------------+   +-------------------------------------+
--       |C: Utility: Create Function1       |   |E: Utility: Create Function2         |
--       ++----------------------------------+   ++-----------------------------------++
--        +----------------------------------+    +-----------------------------------+
--        |D: ProcessUtility: Create Function|    |F: ProcessUtility: Create Function2|
--        +----------------------------------+    +-----------------------------------+

-- Extract span_ids, start and end of those spans
SELECT span_id AS span_a_id,
        get_epoch(span_start) as span_a_start,
        get_epoch(span_end) as span_a_end
		from pg_tracing_peek_spans where parent_id='0000000000000001' and span_type='Utility query' \gset

SELECT span_id AS span_b_id,
        get_epoch(span_start) as span_b_start,
        get_epoch(span_end) as span_b_end
		from pg_tracing_peek_spans where parent_id=:'span_a_id' and span_type='ProcessUtility' \gset

SELECT span_id AS span_c_id,
        get_epoch(span_start) as span_c_start,
        get_epoch(span_end) as span_c_end
		from pg_tracing_peek_spans where parent_id=:'span_b_id' and span_type='Utility query' limit 1 \gset

SELECT span_id AS span_d_id,
        get_epoch(span_start) as span_d_start,
        get_epoch(span_end) as span_d_end
		from pg_tracing_peek_spans where parent_id=:'span_c_id' and span_type='ProcessUtility' \gset

SELECT span_id AS span_e_id,
        get_epoch(span_start) as span_e_start,
        get_epoch(span_end) as span_e_end
		from pg_tracing_peek_spans where parent_id=:'span_b_id' and span_type='Utility query' limit 1 offset 1 \gset

-- Check that the start and end of those spans are within expectation
SELECT :span_a_start <= :span_b_start AS span_a_starts_first,
		:span_a_end >= :span_b_end AS span_a_ends_last,

		:span_d_end <= :span_c_end AS nested_span_ends_before_parent,
		:span_c_end <= :span_e_start AS next_utility_starts_after;

-- Clean current spans
CALL clean_spans();

--
-- Test that no utility is captured with track_utility off
--

-- Set utility off
SET pg_tracing.track_utility = off;

-- Test utility tracking disabled + full sampling
SET pg_tracing.sample_rate = 1.0;
DROP EXTENSION pg_tracing;
CREATE EXTENSION pg_tracing;
SET pg_tracing.sample_rate = 0.0;

-- View displaying spans with their nested level
CREATE VIEW peek_spans_with_level AS
    WITH RECURSIVE list_trace_spans(trace_id, parent_id, span_id, query_id, span_type, span_operation, span_start, span_end, sql_error_code, userid, dbid, pid, subxact_count, plan_startup_cost, plan_total_cost, plan_rows, plan_width, rows, nloops, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, blk_read_time, blk_write_time, temp_blks_read, temp_blks_written, temp_blk_read_time, temp_blk_write_time, wal_records, wal_fpi, wal_bytes, jit_functions, jit_generation_time, jit_inlining_time, jit_optimization_time, jit_emission_time, startup, parameters, deparse_info, lvl) AS (
        SELECT p.*, 1
        FROM pg_tracing_peek_spans p where not parent_id=ANY(SELECT span_id from pg_tracing_peek_spans)
      UNION ALL
        SELECT s.*, lvl + 1
        FROM pg_tracing_peek_spans s, list_trace_spans st
        WHERE s.parent_id = st.span_id
    ) SELECT * FROM list_trace_spans;

-- Create utility view to keep order stable
CREATE VIEW peek_ordered_spans AS
    WITH oldest_start AS (
        SELECT min(span_start) as min_start
        FROM pg_tracing_peek_spans
    ) select *,
        extract(MICROSECONDS FROM age(span_start, oldest_start.min_start)) as us_start,
        extract(MICROSECONDS FROM age(span_end, oldest_start.min_start)) as us_end
        FROM peek_spans_with_level, oldest_start order by span_start, lvl, span_end, deparse_info;

-- Column type to convert json to record
create type span_json as ("traceId" text, "parentSpanId" text, "spanId" text, name text, "startTimeUnixNano" text, "endTimeUnixNano" text, attributes jsonb, kind int, status jsonb);
CREATE VIEW peek_json_spans AS
SELECT *, jsonb_path_query_first(pg_tracing_json_spans()::jsonb, '$.resourceSpans[0].resource.attributes[0].value.stringValue') as service_name
    FROM jsonb_populate_recordset(null::span_json, (SELECT jsonb_path_query_array(pg_tracing_json_spans()::jsonb, '$.resourceSpans[0].scopeSpans[*].spans[*]')));

CREATE FUNCTION get_int_attribute(attributes jsonb, keyvalue text) returns bigint
    LANGUAGE SQL
    IMMUTABLE
    RETURN (jsonb_path_query_first(jsonb_path_query_first(attributes, '$ ? (@.key == $key)', jsonb_build_object('key', keyvalue)),
                                   '$.value.intValue'))::bigint;

CREATE FUNCTION get_double_attribute(attributes jsonb, keyvalue text) returns double precision
    LANGUAGE SQL
    IMMUTABLE
    RETURN (jsonb_path_query_first(jsonb_path_query_first(attributes, '$ ? (@.key == $key)', jsonb_build_object('key', keyvalue)),
                                   '$.value.doubleValue'))::double precision;

CREATE FUNCTION get_string_attribute(attributes jsonb, keyvalue text) returns text
    LANGUAGE SQL
    IMMUTABLE
    RETURN (jsonb_path_query_first(jsonb_path_query_first(attributes, '$ ? (@.key == $key)', jsonb_build_object('key', keyvalue)),
                                   '$.value.stringValue')) ->>0;

CREATE FUNCTION get_string_array_attribute(attributes jsonb, keyvalue text) returns text[]
    LANGUAGE SQL
    IMMUTABLE
    RETURN (SELECT NULLIF(ARRAY(SELECT jsonb_array_elements_text(jsonb_path_query_array(
                jsonb_path_query_first(attributes,
                                       '$ ? (@.key == $key)', jsonb_build_object('key', keyvalue)),
                                       '$.value.arrayValue.values[*].stringValue[*]'))), '{}'));

-- View spans generated from json with their nested level
-- TODO: There's probably a better way to do this...
CREATE VIEW peek_json_spans_with_level AS
WITH RECURSIVE list_trace_spans(trace_id, parent_id, span_id, name, span_start, span_end,
    kind, service_name, query_id, pid, userid, dbid, sql_error_code, subxact_count,
    status_code, status_message,
    plan_startup_cost, plan_total_cost, plan_rows, plan_width,
    rows, nloops,
    shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written,
    local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written,
    blk_read_time, blk_write_time,
    temp_blks_read, temp_blks_written, temp_blk_read_time, temp_blk_write_time,
    wal_records, wal_fpi, wal_bytes,
    jit_functions, jit_generation_time, jit_inlining_time, jit_emission_time,
    startup, parameters, deparse_info,
    lvl) AS (
        SELECT p."traceId", p."parentSpanId", p."spanId", p."name", p."startTimeUnixNano", p."endTimeUnixNano", p.kind,
            p.service_name,
            get_int_attribute(p.attributes, 'query.query_id'),
            get_int_attribute(p.attributes, 'backend.pid'),
            get_int_attribute(p.attributes, 'backend.user_id'),
            get_int_attribute(p.attributes, 'backend.database_id'),
            get_string_attribute(p.attributes, 'query.sql_error_code'),
            get_int_attribute(p.attributes, 'query.subxact_count'),
            p.status -> 'code',
            p.status -> 'message',
            get_double_attribute(p.attributes, 'plan.cost.startup'),
            get_double_attribute(p.attributes, 'plan.cost.total'),
            get_double_attribute(p.attributes, 'plan.rows'),
            get_int_attribute(p.attributes, 'plan.width'),
            get_int_attribute(p.attributes, 'node.rows'),
            get_int_attribute(p.attributes, 'node.nloops'),
            get_int_attribute(p.attributes, 'blocks.shared.hit'),
            get_int_attribute(p.attributes, 'blocks.shared.read'),
            get_int_attribute(p.attributes, 'blocks.shared.dirtied'),
            get_int_attribute(p.attributes, 'blocks.shared.written'),
            get_int_attribute(p.attributes, 'blocks.local.hit'),
            get_int_attribute(p.attributes, 'blocks.local.read'),
            get_int_attribute(p.attributes, 'blocks.local.dirtied'),
            get_int_attribute(p.attributes, 'blocks.local.written'),
            get_double_attribute(p.attributes, 'blocks.io.read_time'),
            get_double_attribute(p.attributes, 'blocks.io.write_time'),
            get_double_attribute(p.attributes, 'temp_blocks.io.read_time'),
            get_double_attribute(p.attributes, 'temp_blocks.io.write_time'),
            get_int_attribute(p.attributes, 'temp_blocks.read'),
            get_int_attribute(p.attributes, 'temp_blocks.written'),
            get_int_attribute(p.attributes, 'wal.records'),
            get_int_attribute(p.attributes, 'wal.fpi'),
            get_int_attribute(p.attributes, 'wal.bytes'),
            get_int_attribute(p.attributes, 'jit.generation_counter'),
            get_double_attribute(p.attributes, 'jit.inlining_counter'),
            get_double_attribute(p.attributes, 'jit.optimization_counter'),
            get_double_attribute(p.attributes, 'jit.emission_counter'),
            get_double_attribute(p.attributes, 'query.startup'),
            get_string_array_attribute(p.attributes, 'query.parameters'),
            get_string_attribute(p.attributes, 'query.deparse_info'),
            1
        FROM peek_json_spans p where not "parentSpanId"=ANY(SELECT span_id from pg_tracing_peek_spans)
      UNION ALL
        SELECT s."traceId", s."parentSpanId", s."spanId", s."name", s."startTimeUnixNano", s."endTimeUnixNano", s.kind,
            s.service_name,
            get_int_attribute(s.attributes, 'query.query_id'),
            get_int_attribute(s.attributes, 'backend.pid'),
            get_int_attribute(s.attributes, 'backend.user_id'),
            get_int_attribute(s.attributes, 'backend.database_id'),
            get_string_attribute(s.attributes, 'query.sql_error_code'),
            get_int_attribute(s.attributes, 'query.subxact_count'),
            s.status -> 'code',
            s.status -> 'message',
            get_double_attribute(s.attributes, 'plan.cost.startup'),
            get_double_attribute(s.attributes, 'plan.cost.total'),
            get_double_attribute(s.attributes, 'plan.rows'),
            get_int_attribute(s.attributes, 'plan.width'),
            get_int_attribute(s.attributes, 'node.rows'),
            get_int_attribute(s.attributes, 'node.nloops'),
            get_int_attribute(s.attributes, 'blocks.shared.hit'),
            get_int_attribute(s.attributes, 'blocks.shared.read'),
            get_int_attribute(s.attributes, 'blocks.shared.dirtied'),
            get_int_attribute(s.attributes, 'blocks.shared.written'),
            get_int_attribute(s.attributes, 'blocks.local.hit'),
            get_int_attribute(s.attributes, 'blocks.local.read'),
            get_int_attribute(s.attributes, 'blocks.local.dirtied'),
            get_int_attribute(s.attributes, 'blocks.local.written'),
            get_double_attribute(s.attributes, 'blocks.io.read_time'),
            get_double_attribute(s.attributes, 'blocks.io.write_time'),
            get_double_attribute(s.attributes, 'temp_blocks.io.read_time'),
            get_double_attribute(s.attributes, 'temp_blocks.io.write_time'),
            get_int_attribute(s.attributes, 'temp_blocks.read'),
            get_int_attribute(s.attributes, 'temp_blocks.written'),
            get_int_attribute(s.attributes, 'wal.records'),
            get_int_attribute(s.attributes, 'wal.fpi'),
            get_int_attribute(s.attributes, 'wal.bytes'),
            get_int_attribute(s.attributes, 'jit.generation_counter'),
            get_double_attribute(s.attributes, 'jit.inlining_counter'),
            get_double_attribute(s.attributes, 'jit.optimization_counter'),
            get_double_attribute(s.attributes, 'jit.emission_counter'),
            get_double_attribute(s.attributes, 'query.startup'),
            get_string_array_attribute(s.attributes, 'query.parameters'),
            get_string_attribute(s.attributes, 'query.deparse_info'),
            lvl + 1
        FROM peek_json_spans s, list_trace_spans st
        WHERE s."parentSpanId" = st.span_id
    ) SELECT * FROM list_trace_spans;

CREATE VIEW peek_ordered_json_spans AS
SELECT * FROM peek_json_spans_with_level order by span_start, lvl, span_end;

-- Nothing should have been generated
select count(*) = 0 from pg_tracing_consume_spans;

-- Prepare and execute a prepared statement
PREPARE test_prepared_one_param (integer) AS SELECT $1;
EXECUTE test_prepared_one_param(100);

-- Nothing should be generated
select count(*) = 0 from pg_tracing_consume_spans;

-- Force a query to start from ExecutorRun
SET plan_cache_mode='force_generic_plan';
EXECUTE test_prepared_one_param(200);
SET plan_cache_mode TO DEFAULT;

-- Again, nothing should be generated
select count(*) = 0 from pg_tracing_consume_spans;

--
-- Test that no utility is captured with track_utility off
--

-- Enable utility tracking and track everything
SET pg_tracing.track_utility = on;
SET pg_tracing.sample_rate = 1.0;

-- Prepare and execute a prepared statement
PREPARE test_prepared_one_param_2 (integer) AS SELECT $1;
EXECUTE test_prepared_one_param_2(100);

-- Check the number of generated spans
select count(distinct(trace_id)) from pg_tracing_peek_spans;
-- Check spans of test_prepared_one_param_2 execution
select span_operation, parameters, lvl from peek_ordered_spans;
-- Check the top span (standalone top span has trace_id=parent_id)
select span_operation, parameters, lvl from peek_ordered_spans where right(trace_id, 16) = parent_id;
CALL clean_spans();

-- Test prepare with table modification
PREPARE test_insert (integer, text) AS INSERT INTO pg_tracing_test(a, b) VALUES ($1, $2);
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ EXECUTE test_insert(100, '2');
-- Check spans of test_insert execution
select trace_id, span_operation, parameters, lvl from peek_ordered_spans WHERE trace_id='00000000000000000000000000000001';
-- We should have only two query_ids
SELECT count(distinct query_id)=2 from pg_tracing_peek_spans where trace_id='00000000000000000000000000000001';
SELECT query_id from pg_tracing_peek_spans where trace_id='00000000000000000000000000000001' AND span_operation = 'ProcessUtility' \gset
SELECT query_id = :query_id from pg_tracing_peek_spans where trace_id='00000000000000000000000000000001' AND span_operation = 'Commit';
CALL clean_spans();

-- Test prepared statement with generic plan
SET plan_cache_mode='force_generic_plan';
EXECUTE test_prepared_one_param(200);
SET plan_cache_mode TO DEFAULT;

-- Check the number of generated spans
select count(distinct(trace_id)) from pg_tracing_peek_spans;
-- Check spans of test_prepared_one_param execution
select span_operation, parameters, lvl from peek_ordered_spans;
-- Check the top span (standalone top span has trace_id=parent_id)
select span_operation, parameters, lvl from peek_ordered_spans where right(trace_id, 16) = parent_id;
CALL clean_spans();

-- Second create extension should generate an error that is captured by span
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ CREATE EXTENSION pg_tracing;
select span_operation, parameters, sql_error_code, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000001';

-- Create test table
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ CREATE TABLE test_create_table (a int, b char(20));
-- Check create table spans
select trace_id, span_type, span_operation, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000002';

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ CREATE INDEX test_create_table_index ON test_create_table (a);
-- Check create index spans
select trace_id, span_type, span_operation, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000003';

CREATE OR REPLACE FUNCTION function_with_error(IN anyarray, OUT x anyelement, OUT n int)
    RETURNS SETOF RECORD
    LANGUAGE sql STRICT IMMUTABLE PARALLEL SAFE
    AS 'select s from pg_catalog.generate_series(1, 1, 1) as g(s)';

-- Check that tracing a function call with the wrong number of arguments is managed correctly
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000004-0000000000000004-01'*/ select function_with_error('{1,2,3}'::int[]);

-- Check lazy function call with error
select trace_id, span_type, span_operation, sql_error_code, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000004';

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000005-0000000000000005-01'*/ ANALYZE test_create_table;
select trace_id, span_type, span_operation, sql_error_code, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000005';

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000006-0000000000000006-01'*/ DROP TABLE test_create_table;
select trace_id, span_type, span_operation, sql_error_code, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000006';

-- Check VACUUM ANALYZE call
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000007-0000000000000007-01'*/ VACUUM ANALYZE pg_tracing_test;
select trace_id, span_type, span_operation, sql_error_code, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000007';

-- Cleanup
CALL clean_spans();
CALL reset_settings();
CALL reset_pg_tracing_test_table();
