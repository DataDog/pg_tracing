-- Check that parameters are not exported when disabled
SET pg_tracing.max_parameter_size=0;
/*traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ select 1, 2, 3;
SELECT span_operation, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000001';

-- Saturate the parameter buffer
SET pg_tracing.max_parameter_size=1;
/*traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ select 1, 2, 3;
SELECT span_operation, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000002';

SET pg_tracing.max_parameter_size=2;
/*traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ select 1, 2, 3;
SELECT span_operation, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000003';

SET pg_tracing.max_parameter_size=3;
/*traceparent='00-00000000000000000000000000000004-0000000000000004-01'*/ select 1, 2, 3;
SELECT span_operation, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000004';

SET pg_tracing.max_parameter_size=4;
/*traceparent='00-00000000000000000000000000000005-0000000000000005-01'*/ select 1, 2, 3;
SELECT span_operation, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000005';

SET pg_tracing.max_parameter_size=5;
/*traceparent='00-00000000000000000000000000000006-0000000000000006-01'*/ select 1, 2, 3;
SELECT span_operation, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000006';
CALL clean_spans();

-- Test truncated string
SET pg_tracing.max_parameter_size=2;
/*traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ select 'testtruncatedstring';
SELECT span_operation, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000001';

SET pg_tracing.max_parameter_size=19;
/*traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ select 'testtruncatedstring';
SELECT span_operation, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000002';

SET pg_tracing.max_parameter_size=20;
/*traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ select 'testtruncatedstring';
SELECT span_operation, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000003';

SET pg_tracing.max_parameter_size=21;
/*traceparent='00-00000000000000000000000000000004-0000000000000004-01'*/ select 'testtruncatedstring';
SELECT span_operation, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000004';

-- Cleanup
CALL clean_spans();
CALL reset_settings();
