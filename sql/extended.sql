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
