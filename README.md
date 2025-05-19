# pg_tracing

![nested_loop trace](https://gist.githubusercontent.com/bonnefoa/c4204828fff8ff1d4ed2b275fbbbdfaa/raw/313dd65703aba3a53da5eaacd1c447ef64ec7bef/nested_loop.png)

pg_tracing is a PostgreSQL extension that generates server-side spans for distributed tracing.

When pg_tracing is active, it generates spans on sampled queries. To access these spans, the extension provides two ways:
- `pg_tracing_consume_spans` and `pg_tracing_peek_spans` views output spans as a set of records
- `pg_tracing_json_spans` function outputs spans as a OTLP json

The utility functions `pg_tracing_reset` and `pg_tracing_info` provide ways to read and reset extension's statistics. These are not available globally but can be enabled for a specific database with `CREATE EXTENSION pg_tracing`.

There are currently two mechanisms to propagate trace context:
- As a SQL comment using [SQLCommenter](https://google.github.io/sqlcommenter/)
- As a GUC parameter `pg_tracing.trace_context`

See [Trace Propagation](#trace-propagation) for more details.

> [!WARNING]
> This extension is still in early development and may be unstable.

## PostgreSQL Version Compatibility

pg_tracing only supports PostgreSQL 14, 15 and 16 for the moment.

## Generated Spans

pg_tracing generates spans for the following events:

- PostgreSQL internal functions: Planner, ProcessUtility, ExecutorRun, ExecutorFinish
- Statements: SELECT, INSERT, DELETE...
- Utility Statements: ALTER, SHOW, TRUNCATE, CALL...
- Execution Plan: A span is created for each node of the execution plan (SeqScan, NestedLoop, HashJoin...)
- Nested queries: Statements invoked within another statement (like a function)
- Triggers: Statements executed through BEFORE and AFTER trigger are tracked
- Parallel Workers: Processes created to handle queries like Parallel SeqScans are tracked
- Transaction Commit: Time spent fsync changes on the WAL

## Documentation

The following list of files is found in the [doc](doc) folder of the pg_tracing github repository. For [installation instructions](#installation), please see the next section of this README.

| File                                                              | Description                                                                   |
|-------------------------------------------------------------------|-------------------------------------------------------------------------------|
| [pg_tracing.md](doc/pg_tracing.md)                                | Main reference documentation for pg_tracing.                                  |


## Installation

### From Source

pg_tracing can be compiled against an installed copy of PostgreSQL with development packages using `PGXS`.

To compile and install the extension, run:

```bash
git clone https://github.com/DataDog/pg_tracing.git
cd pg_tracing
make install
# To compile and install with debug symbols:
PG_CFLAGS="-g" make install
```

## Setup

The extension must be loaded by adding pg_tracing to the [shared_preload_libraries](https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-SHARED-PRELOAD-LIBRARIES) in `postgresql.conf`.
A server restart is needed to add or remove the extension.

```
# postgresql.conf
shared_preload_libraries = 'pg_tracing'

compute_query_id = on
pg_tracing.max_span = 10000
pg_tracing.track = all

# Send spans every 2 seconds to an otel collector
pg_tracing.otel_endpoint = http://127.0.0.1:4318/v1/traces
pg_tracing.otel_naptime = 2000
```

The extension requires additional shared memory proportional to `pg_tracing.max_span`. Note that this memory is consumed whenever the extension is loaded, even if no spans are generated.

## Trace Propagation

### SQLCommenter

Trace context can be propagated through [SQLCommenter](https://google.github.io/sqlcommenter/). By default, all queries with a SQLCommenter with a sampled flag enabled will generate spans.

```sql
-- Query with trace context and sampled flag enable
/*traceparent='00-00000000000000000000000000000123-0000000000000123-01'*/ SELECT 1;

-- Check the generated spans
select trace_id, parent_id, span_id, span_start, span_end, span_type, span_operation from pg_tracing_consume_spans order by span_start;
             trace_id             |    parent_id     |     span_id      |          span_start           |           span_end            |  span_type   | span_operation
----------------------------------+------------------+------------------+-------------------------------+-------------------------------+--------------+----------------
 00000000000000000000000000000123 | 0000000000000123 | 4268a4281c5316dd | 2024-03-19 13:46:43.97958+00  | 2024-03-19 13:46:43.980121+00 | Select query | SELECT $1;
 00000000000000000000000000000123 | 4268a4281c5316dd | 87cb96b6459880a0 | 2024-03-19 13:46:43.979642+00 | 2024-03-19 13:46:43.979978+00 | Planner      | Planner
 00000000000000000000000000000123 | 4268a4281c5316dd | f5994f9159d8e80d | 2024-03-19 13:46:43.980081+00 | 2024-03-19 13:46:43.980111+00 | Executor     | ExecutorRun
```

### `trace_context` GUC

The GUC variable `pg_tracing.trace_context` can also be used to propagate trace context.

```sql
BEGIN;
SET LOCAL pg_tracing.trace_context='traceparent=''00-00000000000000000000000000000005-0000000000000005-01''';
UPDATE pgbench_accounts SET abalance=1 where aid=1;
COMMIT;

-- Check generated span
select trace_id, span_start, span_end, span_type, span_operation from pg_tracing_consume_spans order by span_start;
             trace_id             |          span_start           |           span_end            |     span_type     |                      span_operation
----------------------------------+-------------------------------+-------------------------------+-------------------+-----------------------------------------------------------
 00000000000000000000000000000005 | 2024-07-05 14:24:55.305234+00 | 2024-07-05 14:24:55.305988+00 | Update query      | UPDATE pgbench_accounts SET abalance=$1 where aid=$2;
 00000000000000000000000000000005 | 2024-07-05 14:24:55.305266+00 | 2024-07-05 14:24:55.30552+00  | Planner           | Planner
 00000000000000000000000000000005 | 2024-07-05 14:24:55.305586+00 | 2024-07-05 14:24:55.305906+00 | ExecutorRun       | ExecutorRun
 00000000000000000000000000000005 | 2024-07-05 14:24:55.305591+00 | 2024-07-05 14:24:55.305903+00 | Update            | Update on pgbench_accounts
 00000000000000000000000000000005 | 2024-07-05 14:24:55.305593+00 | 2024-07-05 14:24:55.305806+00 | IndexScan         | IndexScan using pgbench_accounts_pkey on pgbench_accounts
 00000000000000000000000000000005 | 2024-07-05 14:24:55.649757+00 | 2024-07-05 14:24:55.649792+00 | Utility query     | COMMIT;
 00000000000000000000000000000005 | 2024-07-05 14:24:55.649787+00 | 2024-07-05 14:24:55.649792+00 | ProcessUtility    | ProcessUtility
 00000000000000000000000000000005 | 2024-07-05 14:24:55.649816+00 | 2024-07-05 14:24:55.650613+00 | TransactionCommit | TransactionCommit
```

### Standalone Sampling

Queries can also be sampled randomly through the `pg_tracing.sample_rate` parameter. Setting this to 1 will trace all queries.

```sql
-- Enable tracing for all queries
SET pg_tracing.sample_rate = 1.0;

-- Execute a query that will be traced
SELECT 1;

-- Check generated spans
select trace_id, parent_id, span_id, span_start, span_end, span_type, span_operation from pg_tracing_consume_spans order by span_start;
             trace_id             |    parent_id     |     span_id      |          span_start           |           span_end            |  span_type   | span_operation
----------------------------------+------------------+------------------+-------------------------------+-------------------------------+--------------+----------------
 458fbefd7034e670eb3d9c930862c378 | eb3d9c930862c378 | bdecb6e35d429f3d | 2024-01-10 09:54:16.321253+00 | 2024-01-10 09:54:16.321587+00 | Select query | SELECT $1;
 458fbefd7034e670eb3d9c930862c378 | bdecb6e35d429f3d | ad49f27543b0175d | 2024-01-10 09:54:16.3213+00   | 2024-01-10 09:54:16.321412+00 | Planner      | Planner
 458fbefd7034e670eb3d9c930862c378 | bdecb6e35d429f3d | 8805f7749249536b | 2024-01-10 09:54:16.321485+00 | 2024-01-10 09:54:16.321529+00 | Executor     | ExecutorRun
```

## Sending Spans

Spans can be automatically sent to an otel collector by setting the `pg_tracing.otel_endpoint` parameter:

```
# postgresql.conf
pg_tracing.otel_endpoint = http://127.0.0.1:4318/v1/traces
pg_tracing.otel_naptime = 2000
```

If an otel endpoint is defined, a background worker will be started and will send spans every $naptime using the OTLP HTTP/JSON protocol.
The endpoint can be changed while the server is running, but if it was not set when the server was started, changing it to non-empty value
requires a restart.
