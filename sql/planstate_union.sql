-- Test simple append
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/
SELECT count(*) FROM pg_tracing_test WHERE a + 0 < 10
UNION ALL
SELECT count(*) FROM pg_tracing_test WHERE a + 0 < 10000;
SELECT span_operation, deparse_info, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000001';

-- +------------------------------------------+
-- | A: Append                                |
-- +-+-----------------+-+-----------------+--+
--   | B: Aggregate    | | C: Aggregate    |
--   +-----------------+ +-----------------+
--   | D: IndexScan    | | E: IndexScan    |
--   +-----------------+ +-----------------+

SELECT span_id AS span_a_id,
        get_epoch(span_start) as span_a_start,
        get_epoch(span_end) as span_a_end
		from pg_tracing_peek_spans
        where trace_id='00000000000000000000000000000001' AND span_operation='Append' \gset
SELECT span_id AS span_b_id,
        get_epoch(span_start) as span_b_start,
        get_epoch(span_end) as span_b_end
		from pg_tracing_peek_spans
        where parent_id =:'span_a_id' and span_operation='Aggregate' LIMIT 1 OFFSET 0 \gset
SELECT span_id AS span_c_id,
        get_epoch(span_start) as span_c_start,
        get_epoch(span_end) as span_c_end
		from pg_tracing_peek_spans
        where parent_id =:'span_a_id' and span_operation='Aggregate' LIMIT 1 OFFSET 1 \gset

SELECT span_id AS span_d_id,
        get_epoch(span_start) as span_d_start,
        get_epoch(span_end) as span_d_end
		from pg_tracing_peek_spans
        where parent_id =:'span_b_id' \gset
SELECT span_id AS span_e_id,
        get_epoch(span_start) as span_e_start,
        get_epoch(span_end) as span_e_end
		from pg_tracing_peek_spans
        where parent_id =:'span_c_id' \gset

SELECT :span_a_end >= GREATEST(:span_c_end, :span_e_end) as root_ends_last,
       :span_c_start >= :span_b_start as second_index_start_after_first;

-- Cleanup
CALL clean_spans();
CALL reset_settings();
