-- Check generated json for simple and multi line select query
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ SELECT
    1,
2;
-- Check json generated spans for simple and multi line query
SELECT trace_id, name, kind, lvl FROM peek_ordered_json_spans;
CALL clean_spans();

-- Test error code with json export
set statement_timeout=200;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ select * from pg_sleep(1);
-- Check json generated spans with sql error code
SELECT trace_id, name, kind, sql_error_code, lvl FROM peek_ordered_json_spans;
set statement_timeout TO DEFAULT;


-- Cleanup
CALL clean_spans();
