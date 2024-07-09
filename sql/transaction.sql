-- Only trace queries with sample flag
SET pg_tracing.sample_rate = 0.0;
SET pg_tracing.caller_sample_rate = 1.0;

-- Set tracecontext at the start of the transaction
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ BEGIN; SELECT 1; COMMIT;

SELECT span_type, span_operation, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000001' AND span_type!='Commit';
CALL clean_spans();

-- Test with override of the trace context in the middle of a transaction
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ BEGIN;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ SELECT 1; COMMIT;

SELECT span_type, span_operation, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000002' AND span_type!='Commit';
SELECT span_type, span_operation, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000003';
CALL clean_spans();

-- Test with a 0 traceid in the middle of a transaction
BEGIN;
SELECT 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000000-0000000000000001-01'*/ SELECT 2;
SELECT 3;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000000-0000000000000001-01'*/ SELECT 4;
END;

-- Only one trace id should have been generated
SELECT count(distinct(trace_id)) = 1 FROM pg_tracing_peek_spans;
SELECT span_type, span_operation, lvl FROM peek_ordered_spans;
CALL clean_spans();

-- Test with a 0 parent_id in the middle of a transaction
BEGIN;
SELECT 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000000-0000000000000000-01'*/ SELECT 2;
SELECT 3;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000000-0000000000000000-01'*/ SELECT 4;
END;

-- Only one trace id and parent id should have been generated
SELECT count(distinct(trace_id)) = 1, count(distinct(parent_id)) = 1 FROM peek_ordered_spans WHERE lvl=1;
SELECT span_type, span_operation, lvl FROM peek_ordered_spans;

CALL clean_spans();
CALL reset_settings();
