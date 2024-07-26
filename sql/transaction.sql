-- Only trace queries with sample flag
SET pg_tracing.sample_rate = 0.0;
SET pg_tracing.caller_sample_rate = 1.0;

-- Set tracecontext at the start of the transaction
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ BEGIN; SELECT 1; COMMIT;

SELECT span_type, span_operation, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000001' AND span_type!='Commit';
CALL clean_spans();

-- Test with override of the trace context in the middle of a transaction
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ BEGIN;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ SELECT 1; COMMIT;

SELECT span_type, span_operation, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000002' AND span_type!='Commit';
SELECT span_type, span_operation, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000003';
CALL clean_spans();

-- Test with a 0 traceid in the middle of a transaction
BEGIN;
SELECT 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000000-0000000000000001-01'*/ SELECT 2;
SELECT 3;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000000-0000000000000001-01'*/ SELECT 4;
END;

-- Only one trace id should have been generated
SELECT count(distinct(trace_id)) = 1 FROM pg_tracing_peek_spans;
SELECT span_type, span_operation, lvl FROM peek_ordered_spans;
CALL clean_spans();

-- Test with a 0 parent_id in the middle of a transaction
BEGIN;
SELECT 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000000-0000000000000000-01'*/ SELECT 2;
SELECT 3;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000000-0000000000000000-01'*/ SELECT 4;
END;

-- Only one trace id and parent id should have been generated
SELECT count(distinct(trace_id)) = 1, count(distinct(parent_id)) = 1 FROM peek_ordered_spans WHERE lvl=1;
SELECT span_type, span_operation, lvl FROM peek_ordered_spans;
CALL clean_spans();

-- Test modification within transaction block
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000000-01'*/ BEGIN;
INSERT INTO test_modifications(a, b) VALUES (1, 1);
END;
SELECT span_type, span_operation, lvl FROM peek_ordered_spans;

SELECT span_id AS span_tx_block,
        get_epoch(span_start) AS span_tx_block_start,
        get_epoch(span_end) AS span_tx_block_end
		FROM pg_tracing_peek_spans
        WHERE trace_id='00000000000000000000000000000001' AND span_operation='TransactionBlock' \gset
SELECT span_id AS span_commit,
        get_epoch(span_start) as span_commit_start,
        get_epoch(span_end) as span_commit_end
		FROM pg_tracing_peek_spans
        WHERE trace_id='00000000000000000000000000000001'
            AND parent_id = :'span_tx_block'
            AND span_operation='TransactionCommit' \gset
-- Transaction block should end after TransactionCommit span
SELECT :span_tx_block_end >= :span_commit_end;
CALL clean_spans();

-- Test with transaction block created with sample rate
SET pg_tracing.sample_rate = 1.0;
BEGIN;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000002-01'*/ SELECT 2;
COMMIT;
SELECT span_type, span_operation, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000001';

CALL reset_settings();
CALL clean_spans();
