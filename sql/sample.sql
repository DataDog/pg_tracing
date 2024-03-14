-- Trace nothing
SET pg_tracing.sample_rate = 0.0;
SET pg_tracing.caller_sample_rate = 0.0;

-- Query with sampling flag
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT 1;
select count(distinct(trace_id))=0 from pg_tracing_consume_spans;

-- Query without trace context
SELECT 1;
select count(distinct(trace_id))=0 from pg_tracing_consume_spans;

-- Enable full sampling
SET pg_tracing.sample_rate = 1.0;

-- Generate queries with sampling flag on, off and no trace context
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ SELECT 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-00'*/ SELECT 2;
SELECT 3;
SELECT 4;

-- Check number of generated spans
select count(distinct(trace_id)) from pg_tracing_peek_spans;
-- Check span order for full sampling
select span_operation, parameters, lvl from peek_ordered_spans;
-- Top spans should reuse generated ids and have trace_id = parent_id
select span_operation, parameters from peek_ordered_spans where right(trace_id, 16) = parent_id;
CALL clean_spans();

-- Only trace query with sampled flag
SET pg_tracing.sample_rate = 0.0;
SET pg_tracing.caller_sample_rate = 1.0;

-- Clean set call spans
CALL clean_spans();

-- Generate queries with sampling flag on, off, no trace context and SQLComment at the end
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000004-0000000000000004-01'*/ SELECT 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000005-0000000000000005-00'*/ SELECT 2;
SELECT 3 /*dddbs='postgres.db',traceparent='00-00000000000000000000000000000006-0000000000000006-01'*/;
/*dddbs='postgres.db',traceparent='00-fffffffffffffffff000000000000007-0000000000000007-01'*/ SELECT 4;
SELECT 1;

-- Check number of generated spans
select distinct(trace_id) from pg_tracing_peek_spans order by trace_id;
-- Check span order for sampled flag only
select span_operation, parameters, lvl from peek_ordered_spans;
-- Cleaning
CALL clean_spans();

-- Test query filtering
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT 1;
-- Grab query_id for select 1
select distinct(query_id) as query_id_select_1 from pg_tracing_consume_spans \gset
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT 1, 2;
-- Grab query_id for select 1, 2
select distinct(query_id) as query_id_select_1_2 from pg_tracing_consume_spans \gset

SET pg_tracing.filter_query_ids=:'query_id_select_1',:'query_id_select_1_2';
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ SELECT 1, 2;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ SELECT 1, 2, 3;

select count(distinct(query_id)) from pg_tracing_consume_spans;

-- Cleaning
SET pg_tracing.filter_query_ids='';
