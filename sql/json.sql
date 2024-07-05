-- Check generated json for simple and multi line select query
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ SELECT
    1,
2;
-- Check json generated spans for simple and multi line query
SELECT trace_id, name, kind, lvl FROM peek_ordered_json_spans;
-- Test plan attributes with json export
SELECT trace_id, name, plan_startup_cost, plan_total_cost, plan_rows, plan_width, parameters, lvl FROM peek_ordered_json_spans;
CALL clean_spans();

-- Test error code with json export
set statement_timeout=200;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ select * from pg_sleep(1);
-- Check json generated spans with sql error code
set statement_timeout TO DEFAULT;
SELECT trace_id, name, sql_error_code, status_code, status_message, lvl FROM peek_ordered_json_spans;
CALL clean_spans();

-- Test subxact_count and node counters with json export
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ BEGIN;
SAVEPOINT s1;
INSERT INTO pg_tracing_test VALUES(generate_series(1, 2), 'aaa');
ROLLBACK;
SELECT trace_id, name,
    subxact_count,
    shared_blks_hit > 0 as has_shared_hit,
    wal_records > 0 as has_wal_records,
    wal_bytes > 0 as has_wal_bytes,
    startup > 0 as has_startup,
    lvl FROM peek_ordered_json_spans;
CALL clean_spans();

-- Test parameters and deparse_info
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT * FROM pg_tracing_test WHERE a=1;
SELECT trace_id, name, parameters, deparse_info, lvl FROM peek_ordered_json_spans;

-- Cleanup
CALL clean_spans();
