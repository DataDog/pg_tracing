-- Only trace queries with sample flag
SET pg_tracing.sample_rate = 0.0;
SET pg_tracing.caller_sample_rate = 1.0;

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ CREATE TABLE IF NOT EXISTS pg_tracing_test_table_with_constraint (a int, b char(20), CONSTRAINT PK_tracing_test PRIMARY KEY (a));
SELECT span_type, span_operation, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000001';

-- Simple insertion
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ INSERT INTO pg_tracing_test_table_with_constraint VALUES(1, 'aaa');
SELECT span_type, span_operation from peek_ordered_spans where trace_id='00000000000000000000000000000002';

-- Trigger constraint violation
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ INSERT INTO pg_tracing_test_table_with_constraint VALUES(1, 'aaa');
SELECT span_type, span_operation, sql_error_code, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000003';

-- Trigger an error while calling pg_tracing_peek_spans which resets tracing, nothing should be generated
CALL clean_spans();
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000004-0000000000000004-01'*/ INSERT INTO pg_tracing_test_table_with_constraint VALUES(length((select sql_error_code from public.pg_tracing_peek_spans)), 'aaa');
SELECT span_type, span_operation, sql_error_code from peek_ordered_spans where trace_id='00000000000000000000000000000004';

-- Cleanup
CALL clean_spans();
