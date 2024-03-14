-- Enable full sampling
SET pg_tracing.sample_rate = 1.0;

-- Start a transaction with subxaction
BEGIN;
SAVEPOINT s1;
INSERT INTO pg_tracing_test VALUES(generate_series(1, 2), 'aaa');
SAVEPOINT s2;
INSERT INTO pg_tracing_test VALUES(generate_series(1, 2), 'aaa');
SAVEPOINT s3;
SELECT 1;
COMMIT;

-- Check that subxact_count is correctly reported
select span_operation, parameters, subxact_count, lvl FROM peek_ordered_spans;

-- Cleaning
CALL clean_spans();
