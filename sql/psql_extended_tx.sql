-- Only trace individual statements using unnamed prepared statement
BEGIN;
/*traceparent='00-00000000000000000000000000000002-0000000000000001-01'*/ select $1 \parse ''
\bind_named '' 1 \g
/*traceparent='00-00000000000000000000000000000003-0000000000000001-01'*/ select $1, $2 \parse ''
\bind_named '' 1 2 \g
/*traceparent='00-00000000000000000000000000000004-0000000000000001-01'*/ select $1, $2, $3 \parse ''
\bind_named '' 1 2 3 \g
COMMIT;
SELECT trace_id, span_type, span_operation, parameters, lvl FROM peek_ordered_spans;
CALL clean_spans();

-- Mix begin in simple protocol with extended protocol usage
/*traceparent='00-00000000000000000000000000000005-0000000000000001-01'*/ BEGIN;
select $1 \parse ''
\bind_named '' 1 \g
select $1, $2 \parse ''
\bind_named '' 1 2 \g
select $1, $2, $3 \parse ''
\bind_named '' 1 2 3 \g
COMMIT;
SELECT trace_id, span_type, span_operation, parameters, lvl FROM peek_ordered_spans;
CALL clean_spans();

-- Test using extended protocol for utility statement
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000006-0000000000000001-01'*/ BEGIN \parse begin_stmt
\bind_named begin_stmt \g
SELECT 1 \parse stmt_mix_1
\bind_named stmt_mix_1 \g
SELECT 1, 2 \parse stmt_mix_2
\bind_named stmt_mix_2 \g
COMMIT \parse commit_stmt
\bind_named commit_stmt \g

SELECT span_type, span_operation, parameters, lvl FROM peek_ordered_spans
    where trace_id='00000000000000000000000000000006' AND lvl <= 2;
SELECT get_epoch(span_end) as span_select_1_end
	from pg_tracing_peek_spans where span_operation='SELECT $1' and trace_id='00000000000000000000000000000006' \gset
SELECT get_epoch(span_start) as span_select_2_start,
       get_epoch(span_end) as span_select_2_end
	from pg_tracing_peek_spans where span_operation='SELECT $1, $2' and trace_id='00000000000000000000000000000006' \gset
SELECT get_epoch(span_start) as span_commit_start
	from pg_tracing_peek_spans where span_operation='COMMIT' and trace_id='00000000000000000000000000000006' \gset
SELECT :span_select_1_end <= :span_select_2_start,
    :span_select_2_end <= :span_commit_start;
CALL clean_spans();

-- Test with extended protocol while reusing prepared stmts
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ BEGIN;
\bind_named stmt_mix_1 \g
\bind_named stmt_mix_2 \g
SELECT 1, 2, 3 \parse stmt_mix_3
\bind_named stmt_mix_3 \g
\bind_named commit_stmt \g

SELECT span_type, span_operation, parameters, lvl FROM peek_ordered_spans;
SELECT get_epoch(span_end) as span_select_1_end
	from pg_tracing_peek_spans where span_operation='SELECT 1' and trace_id='00000000000000000000000000000001' \gset
SELECT get_epoch(span_start) as span_select_2_start,
       get_epoch(span_end) as span_select_2_end
	from pg_tracing_peek_spans where span_operation='SELECT 1, 2' and trace_id='00000000000000000000000000000001' \gset
SELECT get_epoch(span_start) as span_select_3_start,
       get_epoch(span_end) as span_select_3_end
	from pg_tracing_peek_spans where span_operation='SELECT $1, $2, $3' and trace_id='00000000000000000000000000000001' \gset
SELECT get_epoch(span_start) as span_commit_start
	from pg_tracing_peek_spans where span_operation='COMMIT' and trace_id='00000000000000000000000000000001' \gset
SELECT :span_select_1_end <= :span_select_2_start,
    :span_select_2_end <= :span_select_3_start,
    :span_select_3_end <= :span_commit_start;
DEALLOCATE ALL;
CALL clean_spans();

-- Test extended protocol while tracing everything
SET pg_tracing.sample_rate = 1.0;
BEGIN \parse begin_stmt
\bind_named begin_stmt \g
SELECT 1 \parse stmt_mix_1
\bind_named stmt_mix_1 \g
SELECT 1, 2 \parse stmt_mix_2
\bind_named stmt_mix_2 \g
COMMIT \parse commit_stmt
\bind_named commit_stmt \g

SELECT span_type, span_operation, parameters, lvl FROM peek_ordered_spans
    WHERE lvl <= 2;
SELECT get_epoch(span_end) as span_select_1_end
	from pg_tracing_peek_spans where span_operation='SELECT $1' \gset
SELECT get_epoch(span_start) as span_select_2_start,
       get_epoch(span_end) as span_select_2_end
	from pg_tracing_peek_spans where span_operation='SELECT $1, $2' \gset
SELECT get_epoch(span_start) as span_commit_start
	from pg_tracing_peek_spans where span_operation='COMMIT' \gset
SELECT :span_select_1_end <= :span_select_2_start,
    :span_select_2_end <= :span_commit_start;
CALL clean_spans();

-- Cleanup
CALL reset_settings();
CALL clean_spans();
