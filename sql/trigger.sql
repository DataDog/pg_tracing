-- Only trace queries with sample flag
SET pg_tracing.sample_rate = 0.0;
SET pg_tracing.caller_sample_rate = 1.0;

-- Create test tables
CREATE TABLE Employee (
    EmployeeId INT NOT NULL,
    LastName VARCHAR(20) NOT NULL,
    CONSTRAINT PK_Employee PRIMARY KEY (EmployeeId));
CREATE TABLE Employee_Audit (
    EmployeeId INT NOT NULL,
    LastName VARCHAR(20) NOT NULL,
    EmpAdditionTime VARCHAR(20) NOT NULL);

-- Create trigger functions
CREATE OR REPLACE FUNCTION employee_insert_trigger_fnc()
  RETURNS trigger AS
$$
BEGIN
    INSERT INTO Employee_Audit (EmployeeId, LastName, EmpAdditionTime)
         VALUES(NEW.EmployeeId,NEW.LastName,current_date);
RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION before_trigger_fnc()
  RETURNS trigger AS
$$
BEGIN
    PERFORM 'SELECT 1';
RETURN NEW;
END;
$$
LANGUAGE plpgsql;


-- Hook the insert trigger twice after each row
CREATE TRIGGER employee_insert_trigger
  AFTER INSERT
  ON Employee
  FOR EACH ROW
  EXECUTE PROCEDURE employee_insert_trigger_fnc();

CREATE TRIGGER employee_insert_trigger_2
  AFTER INSERT
  ON Employee
  FOR EACH ROW
  EXECUTE PROCEDURE employee_insert_trigger_fnc();


-- Call insert
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ INSERT INTO Employee VALUES(10,'Adams');

--
-- +--------------------------------------------------------------------------------------------------------+
-- | B: INSERT INTO Employee                                                                                |
-- ++----------+-+--------------+---------------------------------------------------------------------------+
--  |C: Planner| |D: ExecutorRun| G: ExecutorFinish                                                       |
--  +----------+ +--------------+--+-----------------------------------+-+--------------------------------+
--                                 |I: INSERT INTO Employee_Audit...   | |N: INSERT INTO Employee_Audit...|
--                                 +-+----------++--------------------++ +--------------------------------+
--                                   |J: Planner||K: ExecutorRun      |  |O: ExecutorRun |
--                                   +----------++--------------------+  +---------------+

-- Gather span_id, span start and span end of call with triggers
SELECT span_id AS span_b_id,
        get_span_start(span_start) as span_b_start,
        get_span_end(span_start) as span_b_end
		from pg_tracing_peek_spans where parent_id='0000000000000001' \gset
SELECT span_id AS span_c_id,
        get_span_start(span_start) as span_c_start,
        get_span_end(span_start) as span_c_end
		from pg_tracing_peek_spans where parent_id=:'span_b_id' and span_type='Planner' \gset
SELECT span_id AS span_d_id,
        get_span_start(span_start) as span_d_start,
        get_span_end(span_start) as span_d_end
		from pg_tracing_peek_spans where parent_id=:'span_b_id' and span_type='Executor' and span_operation='ExecutorRun' \gset

-- Executor Finish, first trigger
SELECT span_id AS span_g_id,
        get_span_start(span_start) as span_g_start,
        get_span_end(span_start) as span_g_end
		from pg_tracing_peek_spans where parent_id=:'span_b_id' and span_type='Executor' and span_operation='ExecutorFinish' \gset
SELECT span_id AS span_i_id,
        get_span_start(span_start) as span_i_start,
        get_span_end(span_start) as span_i_end
		from pg_tracing_peek_spans where parent_id=:'span_g_id' and span_type='Insert query' limit 1 \gset
SELECT span_id AS span_j_id,
        get_span_start(span_start) as span_j_start,
        get_span_end(span_start) as span_j_end
		from pg_tracing_peek_spans where parent_id=:'span_i_id' and span_type='Planner' \gset
SELECT span_id AS span_k_id,
        get_span_start(span_start) as span_k_start,
        get_span_end(span_start) as span_k_end
		from pg_tracing_peek_spans where parent_id=:'span_i_id' and span_type='Executor' and span_operation='ExecutorRun' \gset

-- Executor Finish, second trigger
SELECT span_id AS span_n_id,
        get_span_start(span_start) as span_n_start,
        get_span_end(span_start) as span_n_end
        from pg_tracing_peek_spans where parent_id=:'span_g_id' and span_type='Insert query' limit 1 offset 1 \gset
SELECT span_id AS span_o_id,
        get_span_start(span_start) as span_o_start,
        get_span_end(span_start) as span_o_end
        from pg_tracing_peek_spans where parent_id=:'span_n_id' and span_type='Executor' and span_operation='ExecutorRun' \gset

-- Check that spans' start and end are within expection
-- Planner
SELECT :span_c_start >= :span_b_start as planner_starts_after_root_span;
-- ExecutorRun
SELECT :span_d_start >= :span_c_end as executor_run_starts_after_planner,
       :span_d_end <= :span_g_start as executor_run_ends_before_finish;
-- ExecutorFinish
SELECT :span_g_start <= :span_i_start as executor_finish_starts_before_child,
       :span_g_end >= :span_o_end as executor_finish_ends_after_child;
-- First trigger
SELECT :span_i_end >= :span_n_end as first_trigger_ends_after_child;
-- Second trigger
SELECT :span_o_start >= :span_i_end  as second_trigger_starts_after_first,
       :span_o_end >= :span_o_end as second_trigger_ends_after_child,
       :span_o_end <= :span_g_end as second_trigger_ends_before_executorfinish_end,
       :span_n_end <= :span_g_end as second_trigger_ends_before_root_executorfinish_end;

-- Check span_operation
SELECT span_type, span_operation, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000001';

-- Check count of query_id
SELECT count(distinct query_id) from pg_tracing_consume_spans where trace_id='00000000000000000000000000000001';

-- Test table to test before trigger
CREATE TABLE before_trigger_table (a int);

-- Hook the before trigger
CREATE TRIGGER employee_insert_trigger
  BEFORE INSERT
  ON before_trigger_table
  FOR EACH ROW
  EXECUTE PROCEDURE before_trigger_fnc();

CREATE TRIGGER employee_update_trigger
  BEFORE UPDATE
  ON before_trigger_table
  FOR EACH ROW
  EXECUTE PROCEDURE before_trigger_fnc();

CREATE TRIGGER employee_delete_trigger
  BEFORE DELETE
  ON before_trigger_table
  FOR EACH ROW
  EXECUTE PROCEDURE before_trigger_fnc();

-- Test explain on a before trigger
-- This will go through ExecutorEnd without any parent executor run
/*dddbs='postgres.db',traceparent='00-fed00000000000000000000000000001-0000000000000003-01'*/ explain INSERT INTO before_trigger_table VALUES(10);
SELECT span_type, span_operation, lvl from peek_ordered_spans where trace_id='fed00000000000000000000000000001';

-- Call before trigger insert
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ INSERT INTO before_trigger_table SELECT generate_series(1,2);
SELECT span_type, span_operation, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000002';

-- Call before trigger update
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ UPDATE before_trigger_table set a=1;
SELECT span_type, span_operation, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000003';

-- Call before trigger delete
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000004-0000000000000004-01'*/ DELETE FROM before_trigger_table where a=1;
SELECT span_type, span_operation, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000004';

-- Check count of query_id
SELECT count(distinct query_id) from pg_tracing_consume_spans;
