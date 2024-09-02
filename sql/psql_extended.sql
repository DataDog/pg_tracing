-- Simple query with extended protocol
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT $1, $2 \parse stmt1
\bind_named stmt1 1 2 \g

SELECT trace_id, span_type, span_operation, parameters, lvl FROM peek_ordered_spans;

BEGIN;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000001-01'*/ select $1 \parse ''
\bind_named '' 1 \g
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000001-01'*/ select $1, $2 \parse ''
\bind_named '' 1 2 \g
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000004-0000000000000001-01'*/ select $1, $2, $3 \parse ''
\bind_named '' 1 2 3 \g
COMMIT;

SELECT trace_id, span_type, span_operation, parameters, lvl FROM peek_ordered_spans;

-- Mix begin in simple protocol with extended protocol usage
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000005-0000000000000001-01'*/ BEGIN;
select $1 \parse ''
\bind_named '' 1 \g
select $1, $2 \parse ''
\bind_named '' 1 2 \g
select $1, $2, $3 \parse ''
\bind_named '' 1 2 3 \g
COMMIT;

SELECT span_type, span_operation, parameters, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000005';

-- Test with extended protocol for tx utility
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000006-0000000000000001-01'*/ BEGIN \parse begin_stmt
\bind_named begin_stmt \g
SELECT 1 \parse stmt_mix_1
\bind_named stmt_mix_1 \g
COMMIT \parse commit_stmt
\bind_named commit_stmt \g
SELECT span_type, span_operation, parameters, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000006';

-- Cleanup
CALL clean_spans();
CALL reset_settings();
