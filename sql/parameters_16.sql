-- Saturate the parameter buffer with extended protocol
SET pg_tracing.max_parameter_size=1;
/*traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ select $1, $2, $3 \bind 1 2 3 \g
SELECT span_operation, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000001';

SET pg_tracing.max_parameter_size=2;
/*traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ select $1, $2, $3 \bind 1 2 3 \g
SELECT span_operation, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000002';

SET pg_tracing.max_parameter_size=3;
/*traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ select $1, $2, $3 \bind 1 2 3 \g
SELECT span_operation, parameters, lvl from peek_ordered_spans where trace_id='00000000000000000000000000000003';

-- Cleanup
CALL clean_spans();
CALL reset_settings();
