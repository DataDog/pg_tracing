-- Check multi statement query
SET pg_tracing.sample_rate = 1.0;

-- Test multiple statements in a single query
/*dddbs='postgres.db',traceparent='00-0000000000000000000000000000000c-000000000000000c-01'*/ select 1; select 2;
SELECT span_operation, parameters, lvl from peek_ordered_spans where trace_id='0000000000000000000000000000000c';
CALL clean_spans();

-- Force a multi-query statement with \;
SELECT 1\; SELECT 1, 2;
SELECT span_type, span_operation, parameters, lvl from peek_ordered_spans;
CALL clean_spans();

-- Cleanup
CALL reset_settings();
