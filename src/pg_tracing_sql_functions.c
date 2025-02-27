/*-------------------------------------------------------------------------
 *
 * pg_tracing_sql_functions.c
 * 		sql functions used by pg_tracing
 *
 * IDENTIFICATION
 *	  src/pg_tracing_sql_functions.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "pg_tracing.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/timestamp.h"

PG_FUNCTION_INFO_V1(pg_tracing_info);
PG_FUNCTION_INFO_V1(pg_tracing_spans);
PG_FUNCTION_INFO_V1(pg_tracing_json_spans);
PG_FUNCTION_INFO_V1(pg_tracing_reset);

/*
 * Get an empty pgTracingStats
 */
pgTracingStats
get_empty_pg_tracing_stats(void)
{
	pgTracingStats stats;

	stats.processed_traces = 0;
	stats.processed_spans = 0;
	stats.dropped_traces = 0;
	stats.dropped_spans = 0;
	stats.dropped_str = 0;
	stats.otel_sent_spans = 0;
	stats.otel_failures = 0;
	stats.last_consume = 0;
	stats.stats_reset = GetCurrentTimestamp();
	return stats;
}

/*
 * Add plan counters to the Datum output
 */
static int
add_plan_counters(const PlanCounters * plan_counters, int i, Datum *values)
{
	values[i++] = Float8GetDatumFast(plan_counters->startup_cost);
	values[i++] = Float8GetDatumFast(plan_counters->total_cost);
	values[i++] = Float8GetDatumFast(plan_counters->plan_rows);
	values[i++] = Int32GetDatum(plan_counters->plan_width);
	return i;
}

/*
 * Add node counters to the Datum output
 */
static int
add_node_counters(const NodeCounters * node_counters, int i, Datum *values, bool *nulls)
{
	Datum		wal_bytes;
	char		buf[256];
	double		blk_read_time,
				blk_write_time;
#if PG_VERSION_NUM >= 150000
	double		temp_blk_read_time,
				temp_blk_write_time;
#endif
	double		generation_counter,
				inlining_counter,
				optimization_counter,
				emission_counter;
	int64		jit_created_functions;

	values[i++] = Int64GetDatumFast(node_counters->rows);
	values[i++] = Int64GetDatumFast(node_counters->nloops);

	/* Buffer usage */
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.shared_blks_hit);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.shared_blks_read);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.shared_blks_dirtied);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.shared_blks_written);

	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.local_blks_hit);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.local_blks_read);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.local_blks_dirtied);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.local_blks_written);

#if PG_VERSION_NUM >= 170000
	blk_read_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.shared_blk_read_time);
	blk_write_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.shared_blk_write_time);
#else
	blk_read_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.blk_read_time);
	blk_write_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.blk_write_time);
#endif

	values[i++] = Float8GetDatumFast(blk_read_time);
	values[i++] = Float8GetDatumFast(blk_write_time);

#if PG_VERSION_NUM >= 150000
	temp_blk_read_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.temp_blk_read_time);
	temp_blk_write_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.temp_blk_write_time);
	values[i++] = Float8GetDatumFast(temp_blk_read_time);
	values[i++] = Float8GetDatumFast(temp_blk_write_time);
#else
	nulls[i++] = 1;
	nulls[i++] = 1;
#endif

	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.temp_blks_read);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.temp_blks_written);

	/* WAL usage */
	values[i++] = Int64GetDatumFast(node_counters->wal_usage.wal_records);
	values[i++] = Int64GetDatumFast(node_counters->wal_usage.wal_fpi);
	snprintf(buf, sizeof buf, UINT64_FORMAT, node_counters->wal_usage.wal_bytes);

	/* Convert to numeric. */
	wal_bytes = DirectFunctionCall3(numeric_in,
									CStringGetDatum(buf),
									ObjectIdGetDatum(0),
									Int32GetDatum(-1));
	values[i++] = wal_bytes;

	/* JIT usage */
	generation_counter = INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.generation_counter);
	inlining_counter = INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.inlining_counter);
	optimization_counter = INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.optimization_counter);
	emission_counter = INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.emission_counter);
	jit_created_functions = node_counters->jit_usage.created_functions;

	values[i++] = Int64GetDatumFast(jit_created_functions);
	values[i++] = Float8GetDatumFast(generation_counter);
	values[i++] = Float8GetDatumFast(inlining_counter);
	values[i++] = Float8GetDatumFast(optimization_counter);
	values[i++] = Float8GetDatumFast(emission_counter);

	return i;
}

/*
 * Generate ArrayType with span's parameters
 */
static Datum
generate_array_parameters(const Span * span)
{
	Datum	   *entries;
	ArrayType  *array;
	int			current_entry = 0;
	const char *cursor = shared_str + span->parameter_offset;
	int			total_parameters = span->num_parameters + span->num_truncated_parameters;

	entries = (Datum *) palloc(sizeof(Datum) * total_parameters);
	for (int i = 0; i < span->num_parameters; i++)
	{
		size_t		len_text = strlen(cursor);

		entries[current_entry++] = PointerGetDatum(cstring_to_text_with_len(cursor, len_text));
		cursor += len_text + 1;
	}
	for (int i = 0; i < span->num_truncated_parameters; i++)
	{
		entries[current_entry++] = PointerGetDatum(cstring_to_text_with_len("...", 3));
	}

	/* Use construct_array for PG15 compatibility */
	array = construct_array(entries, total_parameters, TEXTOID,
							-1, false, TYPALIGN_INT);
	return PointerGetDatum(array);
}

/*
 * Build the tuple for a Span and add it to the output
 */
static void
add_result_span(ReturnSetInfo *rsinfo, Span * span)
{
#define PG_TRACING_TRACES_COLS	44
	Datum		values[PG_TRACING_TRACES_COLS] = {0};
	bool		nulls[PG_TRACING_TRACES_COLS] = {0};
	const char *span_type;
	const char *operation_name;
	const char *sql_error_code;
	int			i = 0;
	char		trace_id[33];
	char		parent_id[17];
	char		span_id[17];

	span_type = span_type_to_str(span->type);
	operation_name = get_operation_name(span);
	sql_error_code = unpack_sql_state(span->sql_error_code);

	pg_snprintf(trace_id, 33, UINT64_HEX_PADDED_FORMAT UINT64_HEX_PADDED_FORMAT,
				span->trace_id.traceid_left,
				span->trace_id.traceid_right);
	pg_snprintf(parent_id, 17, UINT64_HEX_PADDED_FORMAT, span->parent_id);
	pg_snprintf(span_id, 17, UINT64_HEX_PADDED_FORMAT, span->span_id);

	Assert(span_type != NULL);
	Assert(operation_name != NULL);
	Assert(sql_error_code != NULL);

	values[i++] = CStringGetTextDatum(trace_id);
	values[i++] = CStringGetTextDatum(parent_id);
	values[i++] = CStringGetTextDatum(span_id);
	values[i++] = UInt64GetDatum(span->query_id);
	values[i++] = CStringGetTextDatum(span_type);
	values[i++] = CStringGetTextDatum(operation_name);
	values[i++] = Int64GetDatumFast(span->start);
	values[i++] = Int64GetDatumFast(span->end);

	values[i++] = CStringGetTextDatum(sql_error_code);
	values[i++] = UInt32GetDatum(span->be_pid);
	values[i++] = ObjectIdGetDatum(span->user_id);
	values[i++] = ObjectIdGetDatum(span->database_id);
	values[i++] = UInt8GetDatum(span->subxact_count);

	/* Only node and top spans have counters */
	if ((span->type >= SPAN_NODE && span->type <= SPAN_TOP_UNKNOWN)
		|| span->type == SPAN_PLANNER)
	{
		i = add_plan_counters(&span->plan_counters, i, values);
		i = add_node_counters(&span->node_counters, i, values, nulls);
		values[i++] = Int64GetDatumFast(span->startup);

		if (span->parameter_offset != -1)
			values[i++] = generate_array_parameters(span);
		else
			nulls[i++] = 1;

		if (span->deparse_info_offset != -1)
			values[i++] = CStringGetTextDatum(shared_str + span->deparse_info_offset);
	}

	for (int j = i; j < PG_TRACING_TRACES_COLS; j++)
		nulls[j] = 1;

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

Datum
pg_tracing_json_spans(PG_FUNCTION_ARGS)
{
	JsonContext json_ctx;

	/* Don't trace this */
	cleanup_tracing();

	LWLockAcquire(pg_tracing_shared_state->lock, LW_SHARED);
	build_json_context(&json_ctx, shared_spans, shared_str, shared_spans->end);
	marshal_spans_to_json(&json_ctx);
	LWLockRelease(pg_tracing_shared_state->lock);

	PG_RETURN_TEXT_P(cstring_to_text(json_ctx.str->data));
}

#if PG_VERSION_NUM < 150000
/*
 * InitMaterializedSRF
 *
 * Pulled from funcapi.c. Flags can be ignored as it will always be 0
 */
static void
InitMaterializedSRF(FunctionCallInfo fcinfo, bits32 flags)
{
	bool		random_access;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate *tupstore;
	MemoryContext old_context,
				per_query_ctx;
	TupleDesc	stored_tupdesc;

	/* check to see if caller supports returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	/*
	 * Store the tuplestore and the tuple descriptor in ReturnSetInfo.  This
	 * must be done in the per-query memory context.
	 */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	old_context = MemoryContextSwitchTo(per_query_ctx);

	if (get_call_result_type(fcinfo, NULL, &stored_tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	random_access = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;

	tupstore = tuplestore_begin_heap(random_access, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = stored_tupdesc;
	MemoryContextSwitchTo(old_context);
}
#endif

/*
 * Return spans as a result set.
 *
 * Accept a consume parameter. When consume is set,
 * we empty the shared buffer and truncate query text.
 */
Datum
pg_tracing_spans(PG_FUNCTION_ARGS)
{
	bool		consume;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Span	   *span;
	LWLockMode	lock_mode = LW_SHARED;

	consume = PG_GETARG_BOOL(0);

	/*
	 * We need an exclusive lock to truncate and empty the shared buffer when
	 * we consume
	 */
	if (consume)
		lock_mode = LW_EXCLUSIVE;

	if (!pg_tracing_shared_state)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_tracing must be loaded via shared_preload_libraries")));
	InitMaterializedSRF(fcinfo, 0);

	/*
	 * If this query was sampled and we're consuming tracing_spans buffer, the
	 * spans will target a query string that doesn't exist anymore in the
	 * shared str. Better abort the sampling and clean ongoing traces. Since
	 * this will be called within an ExecutorRun, we will need to check for
	 * current_trace_spans at the end of the ExecutorRun hook.
	 */
	cleanup_tracing();

	LWLockAcquire(pg_tracing_shared_state->lock, lock_mode);
	for (int i = 0; i < shared_spans->end; i++)
	{
		span = shared_spans->spans + i;
		add_result_span(rsinfo, span);
	}

	/* Consume is set, remove spans from the shared buffer */
	if (consume)
		drop_all_spans_locked();
	LWLockRelease(pg_tracing_shared_state->lock);

	PG_RETURN_VOID();
}

/*
 * Return statistics of pg_tracing.
 */
Datum
pg_tracing_info(PG_FUNCTION_ARGS)
{
#define PG_TRACING_INFO_COLS	8
	pgTracingStats stats;
	TupleDesc	tupdesc;
	Datum		values[PG_TRACING_INFO_COLS] = {0};
	bool		nulls[PG_TRACING_INFO_COLS] = {0};
	int			i = 0;

	if (!pg_tracing_shared_state)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_tracing must be loaded via shared_preload_libraries")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Get a copy of the pg_tracing stats */
	LWLockAcquire(pg_tracing_shared_state->lock, LW_SHARED);
	stats = pg_tracing_shared_state->stats;
	LWLockRelease(pg_tracing_shared_state->lock);

	values[i++] = Int64GetDatum(stats.processed_traces);
	values[i++] = Int64GetDatum(stats.processed_spans);
	values[i++] = Int64GetDatum(stats.dropped_traces);
	values[i++] = Int64GetDatum(stats.dropped_spans);
	values[i++] = Int64GetDatum(stats.otel_sent_spans);
	values[i++] = Int64GetDatum(stats.otel_failures);
	values[i++] = TimestampTzGetDatum(stats.last_consume);
	values[i++] = TimestampTzGetDatum(stats.stats_reset);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * Reset pg_tracing statistics.
 */
Datum
pg_tracing_reset(PG_FUNCTION_ARGS)
{
	/*
	 * Reset statistics for pg_tracing since all entries are removed.
	 */
	pgTracingStats empty_stats = get_empty_pg_tracing_stats();

	LWLockAcquire(pg_tracing_shared_state->lock, LW_EXCLUSIVE);
	pg_tracing_shared_state->stats = empty_stats;
	LWLockRelease(pg_tracing_shared_state->lock);

	PG_RETURN_VOID();
}
