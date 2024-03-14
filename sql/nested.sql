-- Create test function to sample
CREATE OR REPLACE FUNCTION test_function_project_set(a int) RETURNS SETOF oid AS
$BODY$
BEGIN
	RETURN QUERY SELECT oid from pg_class where oid = a;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_function_result(a int, b text) RETURNS void AS
$BODY$
BEGIN
    INSERT INTO pg_tracing_test(a, b) VALUES (a, b);
END;
$BODY$
LANGUAGE plpgsql;


-- Trace a statement with a function call
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000051-0000000000000051-01'*/ select test_function_project_set(1);

-- The test function call will generate the following spans (non exhaustive list):
-- +------------------------------------------------------------+
-- | A: Select test_function_project_set(1);                    |
-- +-+----------+-+------------------------------------------+--+
--   |C: Planner| |E: ExecutorRun                            |
--   +----------+ +-----------+------------------------------+
--                            | I: Select a from b where...  |
--                            +---+--------------+-----------+
--                                |J: ExecutorRun|
--                                +--------------+

-- Gather span_id, span start and span end of function call statement
SELECT span_id AS span_a_id,
        get_span_start(span_start) as span_a_start,
        get_span_end(span_start) as span_a_end
		from pg_tracing_peek_spans where parent_id='0000000000000051' \gset
SELECT span_id AS span_e_id,
        get_span_start(span_start) as span_e_start,
        get_span_end(span_start) as span_e_end
		from pg_tracing_peek_spans where parent_id=:'span_a_id' and span_type='Executor' and span_operation='ExecutorRun' \gset
SELECT span_id AS span_i_id,
        get_span_start(span_start) as span_i_start,
        get_span_end(span_start) as span_i_end
		from pg_tracing_peek_spans where parent_id=:'span_e_id' and span_type='Select query' \gset
SELECT span_id AS span_j_id,
        get_span_start(span_start) as span_j_start,
        get_span_end(span_start) as span_j_end
		from pg_tracing_peek_spans where parent_id=:'span_i_id' and span_operation='ExecutorRun' \gset

-- Check that spans' start and end are within expection
SELECT :span_a_start <= :span_e_start AS top_query_before_run,
		:span_a_end >= :span_e_end AS top_ends_after_run_end;

-- Check that the root span is the longest one
WITH max_end AS (select max(span_end) from pg_tracing_peek_spans)
SELECT span_end = max_end.max from pg_tracing_peek_spans, max_end
    where span_id = :'span_a_id';

-- Check tracking with top tracking
SET pg_tracing.track = 'top';
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000052-0000000000000052-01'*/ select test_function_project_set(1);
SELECT count(*) from pg_tracing_consume_spans where trace_id='00000000000000000000000000000052';

-- Check tracking with no tracking
SET pg_tracing.track = 'none';
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000053-0000000000000053-01'*/ select test_function_project_set(1);
SELECT count(*) from pg_tracing_consume_spans where trace_id='00000000000000000000000000000053';

-- Reset tracking setting
SET pg_tracing.track TO DEFAULT;

-- Create test procedure
CREATE OR REPLACE PROCEDURE sum_one(i int) AS $$
DECLARE
  r int;
BEGIN
  SELECT (i + i)::int INTO r;
END; $$ LANGUAGE plpgsql;

-- Test tracking of procedure with utility tracking enabled
SET pg_tracing.track_utility=on;
/*traceparent='00-00000000000000000000000000000054-0000000000000054-01'*/ CALL sum_one(3);
select span_operation, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000054';

-- Test again with utility tracking disabled
SET pg_tracing.track_utility=off;
/*traceparent='00-00000000000000000000000000000055-0000000000000055-01'*/ CALL sum_one(10);
select span_operation, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000055';

-- Create immutable function
CREATE OR REPLACE FUNCTION test_immutable_function(a int) RETURNS oid
AS 'SELECT oid from pg_class where oid = a;'
LANGUAGE sql IMMUTABLE;

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000056-0000000000000056-01'*/ select test_immutable_function(1);
select span_operation, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000056';

-- +-------------------------------------------------------------------+
-- | A: Select test_function(1);                                       |
-- +-+----------++----------------++-------------------------------+---+
--   |C: Planner||D: ExecutorStart||E: ExecutorRun                 |
--   +----------++----------------+++------------------------------+
--                                   |H: Insert INTO...    |
--                                   +--+--------------+---+
--                                      |I: ExecutorRun|
--                                      +--------------+

-- Check function with result node
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000058-0000000000000058-01'*/ select test_function_result(1, 'test');

-- Gather span_id, span start and span end of function call statement
SELECT span_id AS span_a_id,
        get_span_start(span_start) as span_a_start,
        get_span_end(span_start) as span_a_end
		from pg_tracing_peek_spans where parent_id='0000000000000058' \gset
SELECT span_id AS span_e_id,
        get_span_start(span_start) as span_e_start,
        get_span_end(span_start) as span_e_end
		from pg_tracing_peek_spans where parent_id=:'span_a_id' and span_type='Executor' and span_operation='ExecutorRun' \gset
SELECT span_id AS span_h_id,
        get_span_start(span_start) as span_h_start,
        get_span_end(span_start) as span_h_end
		from pg_tracing_peek_spans where parent_id=:'span_e_id' and span_type='Insert query' \gset

select span_operation, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000058';

-- Cleanup
CALL clean_spans();
