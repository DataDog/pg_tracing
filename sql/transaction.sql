-- Only trace queries with sample flag
SET pg_tracing.sample_rate = 0.0;
SET pg_tracing.caller_sample_rate = 1.0;

-- Set tracecontext at the start of the transaction
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ BEGIN; SELECT 1; COMMIT;

SELECT span_type, span_operation, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000001' AND span_type!='Commit';

-- Test with override of the trace context in the middle of a transaction
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ BEGIN;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ SELECT 1; COMMIT;

SELECT span_type, span_operation, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000002' AND span_type!='Commit';
SELECT span_type, span_operation, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000003';

CALL clean_spans();
CALL reset_settings();
