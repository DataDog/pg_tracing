/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT * from pg_tracing_test where a=1 OR a=2 OR a=3;
SELECT span_operation, deparse_info, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000001';

--
-- +----------------------------------------------------------------------------------------------+
-- | A: BitmapOr                                                                                  |
-- ++-----------------------------+-------------------------------+-------------------------------+
--  |B: Bitmap Index Scan (aid=1) |C: Bitmap Index Scan (aid=2)   |D: Bitmap Index Scan (aid=3)   |
--  +-----------------------------+-------------------------------+-------------------------------+

SELECT span_id AS span_a_id,
        get_span_start(span_start) as span_a_start,
        get_span_end(span_start) as span_a_end
		from pg_tracing_peek_spans
        where trace_id='00000000000000000000000000000001' AND span_operation='BitmapOr' \gset
SELECT span_id AS span_b_id,
        get_span_start(span_start) as span_b_start,
        get_span_end(span_start) as span_b_end
		from pg_tracing_peek_spans
        where parent_id =:'span_a_id' and deparse_info='Index Cond: (a = 1)' \gset
SELECT span_id AS span_c_id,
        get_span_start(span_start) as span_c_start,
        get_span_end(span_start) as span_c_end
		from pg_tracing_peek_spans
        where parent_id =:'span_a_id' and deparse_info='Index Cond: (a = 2)' \gset
SELECT span_id AS span_d_id,
        get_span_start(span_start) as span_d_start,
        get_span_end(span_start) as span_d_end
		from pg_tracing_peek_spans
        where parent_id =:'span_a_id' and deparse_info='Index Cond: (a = 2)' \gset

SELECT :span_b_end >= :span_c_start as bitmap_or_second_child_start_after_first,
       :span_c_end >= :span_d_start as bitmap_or_third_child_start_after_second,
       :span_d_end <= :span_a_end as bitmap_or_ends_after_latest_child;

-- Clean created spans
CALL clean_spans();
