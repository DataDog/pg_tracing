-- Test trace context propagation through GUCs
SET pg_tracing.trace_context='dddbs=''postgres.db'',traceparent=''00-00000000000000000000000000000004-0000000000000004-01';
SELECT 1;

-- Test trace context propagation through a local GUCs
BEGIN;
SET LOCAL pg_tracing.trace_context='dddbs=''postgres.db'',traceparent=''00-00000000000000000000000000000005-0000000000000005-01';
SELECT 1;
COMMIT;

-- Test multiple statements
SET pg_tracing.trace_context='dddbs=''postgres.db'',traceparent=''00-fffffffffffffffffffffffffffffff5-0000000000000005-01';
SELECT 2;
SELECT 3;

-- Check results for GUC propagation with simple and multiple statements
select trace_id, span_operation, parameters, lvl from peek_ordered_spans;
CALL clean_spans();

-- Mix SQLCommenter and GUC propagation
SET pg_tracing.trace_context='dddbs=''postgres.db'',traceparent=''00-fffffffffffffffffffffffffffffff6-0000000000000006-01';
SELECT 2;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT 1, 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000009-0000000000000009-00'*/ SELECT 1, 2, 3;
SELECT 3;

-- Check mix SQLCommenter and GUC propagation
select trace_id, span_operation, parameters, lvl from peek_ordered_spans;
CALL clean_spans();

-- Test statement after reset
SET pg_tracing.trace_context TO default;
SELECT 4;

-- Test no traceparent field
SET pg_tracing.trace_context='dddbs=''postgres.db'',taceparent=''00-fffffffffffffffffffffffffffffff5-0000000000000005-01';
-- Test incorrect trace id
SET pg_tracing.trace_context='dddbs=''postgres.db'',traceparent=''00-ffffffffffffffffffffffffffffff5-0000000000000005-01';
-- Test wrong format
SET pg_tracing.trace_context='dddbs=''postgres.db'',traceparent=''00f-ffffffffffffffffffffffffffffff5-0000000000000005-01';

-- GUC errors and no GUC tracecontext should not generate spans
select count(*) = 0 from peek_ordered_spans;
