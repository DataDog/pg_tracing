-- A simple procedure creating nested calls
CREATE OR REPLACE PROCEDURE loop_select(iterations int) AS
$BODY$
BEGIN
    FOR i IN 1..iterations LOOP
        PERFORM 'SELECT 1;';
    END LOOP;
END;
$BODY$
LANGUAGE plpgsql;

-- Clear stats
select * from pg_tracing_reset();
-- Check initial stats after reset
select processed_traces, processed_spans, dropped_traces, dropped_spans from pg_tracing_info();

-- Saturate the span buffer. Each call should create at least 2 spans
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ CALL loop_select(20);
-- Check that we have dropped spans. The trace was still partially processed
select processed_traces = 1, processed_spans = 40, dropped_traces = 0, dropped_spans > 0 from pg_tracing_info();

-- Try to create new traces while buffer is full
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ SELECT 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-00'*/ SELECT 1;

-- We should have only one additional dropped trace
select processed_traces = 1, processed_spans = 40, dropped_traces = 1 from pg_tracing_info();

-- Clean current spans
CALL clean_spans();
