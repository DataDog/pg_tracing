
-- Test with explain_spans disabled
SET pg_tracing.explain_spans = false;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT s.relation_size + s.index_size
FROM (SELECT
      pg_relation_size(C.oid) as relation_size,
      pg_indexes_size(C.oid) as index_size
    FROM pg_class C) as s limit 1;
SELECT span_type, span_operation, deparse_info FROM peek_ordered_spans where trace_id='00000000000000000000000000000001';

-- Test with explain_spans enabled
SET pg_tracing.explain_spans = true;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/  SELECT s.relation_size + s.index_size
FROM (SELECT
      pg_relation_size(C.oid) as relation_size,
      pg_indexes_size(C.oid) as index_size
    FROM pg_class C) as s limit 1;
SELECT span_type, span_operation, deparse_info FROM peek_ordered_spans where trace_id='00000000000000000000000000000002';

-- Check generated spans when deparse is disabled
SET pg_tracing.deparse_plan=false;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ SELECT * from pg_tracing_test where a=1;
SELECT span_operation, deparse_info, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000003';

-- Check generated spans when deparse is enabled
SET pg_tracing.deparse_plan=true;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000004-0000000000000004-01'*/ SELECT * from pg_tracing_test where a=1;
SELECT span_operation, deparse_info, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000004';

-- Clean created spans
CALL clean_spans();
