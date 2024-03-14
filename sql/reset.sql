-- Check reset is working
SELECT pg_tracing_reset();
SELECT traces from pg_tracing_info;
