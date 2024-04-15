/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ select information_schema._pg_expandarray('{0,1,2}'::int[]);
SELECT span_operation, deparse_info, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000001';

-- +---------------------------------------------+
-- | A: ProjectSet                               |
-- ++-----------------+--+----------------+------+
--  | B: Result       |  | C: TopSpan     |
--  +-----------------+  +-+------------+-+
--                         | D: Planner |
--                         +------------+

SELECT span_id AS span_a_id,
        get_epoch(span_start) as span_a_start,
        get_epoch(span_end) as span_a_end
		from pg_tracing_peek_spans
        where trace_id='00000000000000000000000000000001' AND span_operation='ProjectSet' \gset
SELECT span_id AS span_b_id,
        get_epoch(span_start) as span_b_start,
        get_epoch(span_end) as span_b_end
		from pg_tracing_peek_spans
        where parent_id =:'span_a_id' and span_operation='Result' \gset
SELECT span_id AS span_c_id,
        get_epoch(span_start) as span_c_start,
        get_epoch(span_end) as span_c_end
		from pg_tracing_peek_spans
        where parent_id =:'span_a_id' and span_type='Select query' \gset
SELECT span_id AS span_d_id,
        get_epoch(span_start) as span_d_start,
        get_epoch(span_end) as span_d_end
		from pg_tracing_peek_spans
        where parent_id =:'span_c_id' and span_type='Planner' \gset

SELECT :span_a_end >= :span_c_end as project_set_ends_after_nested_top_span;

-- Clean created spans
CALL clean_spans();
