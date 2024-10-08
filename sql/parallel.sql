begin;
-- encourage use of parallel plans
set local parallel_setup_cost=0;
set local parallel_tuple_cost=0;
set local min_parallel_table_scan_size=0;
set local max_parallel_workers_per_gather=2;

-- Trace parallel queries
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ select 1 from pg_class limit 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000002-01'*/ select 2 from pg_class limit 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000003-00'*/ select 3 from pg_class limit 1;

-- Try with parallel tracing disabled
set local pg_tracing.trace_parallel_workers = false;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000004-01'*/ select 4 from pg_class limit 1;
commit;

-- get tx block
select span_id as tx_block_id from pg_tracing_peek_spans where span_type='TransactionBlock' and trace_id='00000000000000000000000000000001' and parent_id='0000000000000001' \gset
-- get root top span id
select span_id as root_span_id from pg_tracing_peek_spans where span_type='Select query' and trace_id='00000000000000000000000000000001' and parent_id=:'tx_block_id' limit 1 \gset
-- Get executor top span id
SELECT span_id as executor_span_id from pg_tracing_peek_spans where span_operation='ExecutorRun' and trace_id='00000000000000000000000000000001' and parent_id=:'root_span_id' \gset
-- Get Limit span id
SELECT span_id as limit_span_id from pg_tracing_peek_spans where span_operation='Limit' and trace_id='00000000000000000000000000000001' and parent_id=:'executor_span_id' \gset
-- Get Gather span id
SELECT span_id as gather_span_id from pg_tracing_peek_spans where span_operation='Gather' and trace_id='00000000000000000000000000000001' and parent_id=:'limit_span_id' \gset

-- Check the select spans that are attached to the root top span
SELECT trace_id, span_type, span_operation from pg_tracing_peek_spans where span_type='Select query' and parent_id=:'gather_span_id' order by span_operation;

-- Check generated trace_id
SELECT trace_id from pg_tracing_peek_spans group by trace_id;

-- Check number of executor spans
SELECT count(*) from pg_tracing_consume_spans where span_operation='ExecutorRun';
CALL clean_spans();

-- Test leaderless parallel query
set parallel_setup_cost=0;
set parallel_tuple_cost=0;
set min_parallel_table_scan_size=0;
set max_parallel_workers_per_gather=2;
set parallel_leader_participation=false;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ select 1 from pg_class limit 1;

SELECT span_type, span_operation, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000001' ORDER BY lvl, span_operation;

-- Cleanup
CALL clean_spans();
CALL reset_settings();
