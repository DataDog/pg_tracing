-- Test explain on a before trigger
-- This will go through ExecutorEnd without any parent executor run
/*dddbs='postgres.db',traceparent='00-fed00000000000000000000000000001-0000000000000003-01'*/ explain INSERT INTO before_trigger_table VALUES(10);
SELECT trace_id, span_type, span_operation, lvl from peek_ordered_spans where trace_id='fed00000000000000000000000000001';

CALL clean_spans();
