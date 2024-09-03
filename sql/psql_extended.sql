-- Select with extended protocol
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT $1, $2 \parse stmt1
\bind_named stmt1 1 2 \g

SELECT trace_id, span_type, span_operation, parameters, lvl FROM peek_ordered_spans;

WITH max_end AS (select max(span_end) from pg_tracing_peek_spans)
SELECT span_end = max_end.max from pg_tracing_peek_spans, max_end
    where span_type='Select query' AND parameters IS NOT NULL;

-- Cleanup
CALL reset_settings();
CALL clean_spans();
