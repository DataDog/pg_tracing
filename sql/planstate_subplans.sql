-- Test with initplan
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ WITH cte_init AS MATERIALIZED (SELECT 1 a, 'cte_init val' b)
MERGE INTO m USING (select 1 k, 'merge source InitPlan' v offset 0) o ON m.k=o.k
WHEN MATCHED THEN UPDATE SET v = (SELECT b || ' merge update' FROM cte_init WHERE a = 1 LIMIT 1)
WHEN NOT MATCHED THEN INSERT VALUES(o.k, o.v);

-- Check generated spans for init plan
SELECT span_operation, deparse_info, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000001' AND lvl < 4;

-- +----------------------------------------------------------+
-- | A: Merge on m                                            |
-- ++---------------------------------------------------------+
--  | B: Hash Right Join                                |
--  +---------------------------------------------------+
--                              +-------------+
--                              | F: CTE init |
--                              +-------------+
--                              | G: Result   |
--                              +-------------+
--                  +-------------------------------------+
--                  | C: InitPlan                         |
--                  +-----------------------------+-------+
--                  | D: Limit                    |
--                  +----+------------------------+
--                       | E: CTEScan on cte_init |
--                       +------------------------+

SELECT span_id AS span_a_id,
        get_epoch(span_start) as span_a_start,
        get_epoch(span_end) as span_a_end
		from pg_tracing_peek_spans
        where trace_id='00000000000000000000000000000001' AND span_operation='Merge on m' \gset
SELECT span_id AS span_b_id,
        get_epoch(span_start) as span_b_start,
        get_epoch(span_end) as span_b_end
		from pg_tracing_peek_spans
        where parent_id =:'span_a_id' and span_operation='Hash Right Join' \gset
SELECT span_id AS span_c_id,
        get_epoch(span_start) as span_c_start,
        get_epoch(span_end) as span_c_end
		from pg_tracing_peek_spans
        where parent_id =:'span_a_id' and span_operation='InitPlan 2 (returns $1)' \gset
SELECT span_id AS span_d_id,
        get_epoch(span_start) as span_d_start,
        get_epoch(span_end) as span_d_end
		from pg_tracing_peek_spans
        where parent_id =:'span_c_id' and span_operation='Limit' \gset
SELECT span_id AS span_e_id,
        get_epoch(span_start) as span_e_start,
        get_epoch(span_end) as span_e_end
		from pg_tracing_peek_spans
        where parent_id =:'span_d_id' and span_operation='CTEScan on cte_init' \gset
SELECT span_id AS span_f_id,
        get_epoch(span_start) as span_f_start,
        get_epoch(span_end) as span_f_end
		from pg_tracing_peek_spans
        where parent_id =:'span_a_id' and span_operation='CTE cte_init' \gset
SELECT span_id AS span_g_id,
        get_epoch(span_start) as span_g_start,
        get_epoch(span_end) as span_g_end
		from pg_tracing_peek_spans
        where parent_id =:'span_f_id' and span_operation='Result' \gset

SELECT :span_a_end >= :span_c_end as merge_ends_after_init_plan,
       :span_c_end >= :span_d_start as bitmap_or_third_child_start_after_second,
       :span_e_start <= :span_f_start as cte_scan_starts_before_cte_init,
       :span_f_start >= :span_e_start as cte_init_starts_after_cte_scan;

CALL clean_spans();

-- Test with subplan

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ WITH cte_basic AS MATERIALIZED (SELECT 1 a, 'cte_basic val' b)
MERGE INTO m USING (select 1 k, 'merge source SubPlan' v offset 0) o ON m.k=o.k
WHEN MATCHED THEN UPDATE SET v = (SELECT b || ' merge update' FROM cte_basic WHERE cte_basic.a = m.k LIMIT 1)
WHEN NOT MATCHED THEN INSERT VALUES(o.k, o.v);

-- Check generated spans for init plan
-- +---------------------------------------------------------------+
-- | A: Merge on m                                                 |
-- ++--------------------+-----------------------------------------+
--  | B: Hash Right Join |
--  +--------------------+
--                                      +-------------+
--                                      | F: CTE basic|
--                                      +-------------+
--                                      | G: Result   |
--                                      +-------------+
--                          +--------------------------------------+
--                          | C: Subplan 2                         |
--                          +------------------------------+-------+
--                          | D: Limit                     |
--                          +----+-------------------------+
--                               | E: CTEScan on cte_basic |
--                               +-------------------------+

SELECT span_id AS span_a_id,
        get_epoch(span_start) as span_a_start,
        get_epoch(span_end) as span_a_end
		from pg_tracing_peek_spans
        where trace_id='00000000000000000000000000000002' AND span_operation='Merge on m' \gset
SELECT span_id AS span_b_id,
        get_epoch(span_start) as span_b_start,
        get_epoch(span_end) as span_b_end
		from pg_tracing_peek_spans
        where parent_id =:'span_a_id' and span_operation='Hash Right Join' \gset
SELECT span_id AS span_c_id,
        get_epoch(span_start) as span_c_start,
        get_epoch(span_end) as span_c_end
		from pg_tracing_peek_spans
        where parent_id =:'span_a_id' and span_operation='SubPlan 2' \gset
SELECT span_id AS span_d_id,
        get_epoch(span_start) as span_d_start,
        get_epoch(span_end) as span_d_end
		from pg_tracing_peek_spans
        where parent_id =:'span_c_id' and span_operation='Limit' \gset
SELECT span_id AS span_e_id,
        get_epoch(span_start) as span_e_start,
        get_epoch(span_end) as span_e_end
		from pg_tracing_peek_spans
        where parent_id =:'span_d_id' and span_operation='CTEScan on cte_basic' \gset
SELECT span_id AS span_f_id,
        get_epoch(span_start) as span_f_start,
        get_epoch(span_end) as span_f_end
		from pg_tracing_peek_spans
        where parent_id =:'span_a_id' and span_operation='CTE cte_basic' \gset
SELECT span_id AS span_g_id,
        get_epoch(span_start) as span_g_start,
        get_epoch(span_end) as span_g_end
		from pg_tracing_peek_spans
        where parent_id =:'span_f_id' and span_operation='Result' \gset

-- Subplans are stopped by the shutdown node walker after the main node.
-- Therefore, subplan can appear as finishing after its parent
SELECT :span_a_end <= :span_c_end as merge_ends_before_subplan,
       :span_c_end >= :span_d_start as bitmap_or_third_child_start_after_second,
       :span_e_start <= :span_f_start as cte_scan_starts_before_cte_basic,
       :span_f_start >= :span_e_start as cte_basic_starts_after_cte_scan;

-- Check deparse information for subplan
SELECT span_operation, deparse_info
FROM pg_tracing_peek_spans
WHERE trace_id='00000000000000000000000000000002' AND span_id=:'span_e_id';

CALL clean_spans();
