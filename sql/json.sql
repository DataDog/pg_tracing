-- Check generated json for simple select query
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT 1;

-- Check generated json for multi line query
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ SELECT
    1,
2;

-- Check json results
SELECT "traceId", name, kind, lvl FROM peek_ordered_json_spans;

-- Cleanup
CALL clean_spans();
