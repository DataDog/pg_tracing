-- Trace everything
SET pg_tracing.sample_rate = 1.0;

-- Simple query with extended protocol
SELECT $1, $2 \bind 1 2 \g
SELECT span_type, span_operation, parameters, lvl FROM peek_ordered_spans;
CALL clean_spans();

-- Trigger an error due to mismatching number of parameters
BEGIN; select $1 \bind \g
ROLLBACK;
SELECT span_type, span_operation, parameters, lvl FROM peek_ordered_spans;
CALL clean_spans();

-- Execute queries with extended protocol within an explicit transaction
BEGIN;
SELECT $1 \bind 1 \g
SELECT $1, $2 \bind 2 3 \g
COMMIT;

-- Send begin through extended protocol
BEGIN \bind \g
SELECT $1 \bind 1 \g
COMMIT;

-- Spans within the same transaction should have been generated with the same trace_id
SELECT count(distinct(trace_id)) = 1 FROM pg_tracing_peek_spans;
SELECT span_type, span_operation, parameters, lvl FROM peek_ordered_spans;
CALL clean_spans();

-- Mix extended protocol and simple protocol
BEGIN;
SELECT $1 \bind 1 \g
SELECT 5, 6, 7;
SELECT $1, $2 \bind 2 3 \g
COMMIT;

-- Spans within the same transaction should have been generated with the same trace_id
SELECT count(distinct(trace_id)) = 1 FROM pg_tracing_peek_spans;
SELECT span_type, span_operation, parameters, lvl FROM peek_ordered_spans;
CALL clean_spans();

-- gdesc calls a single parse command then execute a query. Make sure we handle this case
SELECT 1 \gdesc
SELECT span_type, span_operation, parameters, lvl FROM peek_ordered_spans;
CALL clean_spans();

-- Trace only sampled statements
SET pg_tracing.sample_rate = 0.0;

-- Test tracing the whole transaction with extended protocol
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ BEGIN;
SELECT $1 \bind 1 \g
SELECT $1, $2 \bind 1 2 \g
SELECT $1, $2, $3 \bind 1 2 3 \g
COMMIT;

SELECT span_type, span_operation, parameters, lvl FROM peek_ordered_spans WHERE trace_id='00000000000000000000000000000001';

-- Test tracing only individual statements with extended protocol
BEGIN;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000001-01'*/ SELECT $1 \bind 1 \g
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ SELECT $1, $2 \bind 1 2 \g
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000003-01'*/ SELECT $1, $2, $3 \bind 1 2 3 \g
COMMIT;

SELECT span_type, span_operation, parameters, lvl FROM peek_ordered_spans WHERE trace_id='00000000000000000000000000000002';

-- Test tracing only subset of individual statements with extended protocol
BEGIN;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000001-01'*/ SELECT $1 \bind 1 \g
SELECT $1, $2 \bind 1 2 \g
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ SELECT $1, $2, $3 \bind 1 2 3 \g
SELECT $1 \bind 1 \g
COMMIT;

SELECT span_type, span_operation, parameters, lvl FROM peek_ordered_spans WHERE trace_id='00000000000000000000000000000003';

-- Test tracing the whole transaction with extended protocol with BEGIN sent through extended protocol
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000004-0000000000000004-01'*/ BEGIN \bind \g
SELECT $1 \bind 1 \g
COMMIT;
SELECT span_type, span_operation, parameters, lvl FROM peek_ordered_spans WHERE trace_id='00000000000000000000000000000004';

-- Cleanup
CALL clean_spans();
CALL reset_settings();
