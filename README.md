# pg_tracing

![nested_loop trace](https://gist.githubusercontent.com/bonnefoa/c4204828fff8ff1d4ed2b275fbbbdfaa/raw/313dd65703aba3a53da5eaacd1c447ef64ec7bef/nested_loop.png)

pg_tracing is a PostgreSQL extension allows to generate server-side spans for distributed tracing.

When pg_tracing is active, it generates spans on sampled queries. To access these spans, the extension provides two views: `pg_tracing_consume_spans` and `pg_tracing_peek_spans`. The utility functions `pg_tracing_reset` and `pg_tracing_info` provide ways to read and reset extension's statistics. These are not available globally but can be enabled for a specific database with `CREATE EXTENSION pg_tracing`.

Trace propagation currently relies on [SQLCommenter](https://google.github.io/sqlcommenter/). More mechanisms will be added in the future.

> [!WARNING]  
> This extension is still in early development and may be unstable.

## PostgreSQL Version Compatibility

pg_tracing only supports PostgreSQL 15 and 16 for the moment.

## Generated Spans

pg_tracing generates spans for the following events:

- PostgreSQL internal functions: Planner, ProcessUtility, ExecutorRun, ExecutorFinish
- Statements: SELECT, INSERT, DELETE...
- Utility Statements: ALTER, SHOW, TRUNCATE, CALL...
- Execution Plan: A span is created for each node of the execution plan (SeqScan, NestedLoop, HashJoin...)
- Nested queries: Statements invoked within another statement (like a function)
- Triggers: Statements executed through BEFORE and AFTER trigger are tracked
- Parallel Workers: Processes created to handle queries like Parallel SeqScans are tracked 

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
```

## Setup

The extension must be loaded by adding pg_tracing to the [shared_preload_libraries](https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-SHARED-PRELOAD-LIBRARIES) in `postgresql.conf`. A server restart is needed to add or remove the extension.

```
# postgresql.conf
shared_preload_libraries = 'pg_tracing'

compute_query_id = on
pg_tracing.max_span = 10000
pg_tracing.track = all
```

The extension requires additional shared memory proportional to `pg_tracing.max_span`. Note that this memory is consumed whenever the extension is loaded, even if no spans are generated.

When `pg_tracing` is active, it generates spans on sampled queries. To access these spans, the extension provides two views: `pg_tracing_consume_spans` and `pg_tracing_peek_spans`. The utility functions `pg_tracing_reset` and `pg_tracing_info` provide ways to read and reset extension's statistics. These are not available globally but can be enabled for a specific database with `CREATE EXTENSION pg_tracing`.

## Usage

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

## Authors

* [Anthonin Bonnefoy](https://github.com/bonnefoa)
