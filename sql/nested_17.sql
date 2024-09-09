-- Test error thrown within a nested function
/*traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT
    eval_expr(format('date %L + interval %L', '-infinity', 'infinity')),
    eval_expr(format('date %L - interval %L', '-infinity', 'infinity'));
select span_operation, lvl FROM peek_ordered_spans where trace_id='00000000000000000000000000000001';

-- Cleanup
CALL clean_spans();
