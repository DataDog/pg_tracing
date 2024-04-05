/*-------------------------------------------------------------------------
 *
 * pg_tracing.c
 *		Generate spans for distributed tracing from SQL query
 *
 * Spans will only be generated for sampled queries. A query is sampled if:
 * - It has a tracecontext propagated throught SQLCommenter and it passes the
 *   caller_sample_rate.
 * - It has no SQLCommenter but the query randomly passes the global sample_rate
 *
 * A query with SQLcommenter will look like: /\*dddbs='postgres.db',
 * traceparent='00-00000000000000000000000000000009-0000000000000005-01'*\/ select 1;
 * The traceparent fields are detailed in
 * https://www.w3.org/TR/trace-context/#traceparent-header-field-values
 * 00000000000000000000000000000009: trace id
 * 0000000000000005: parent id
 * 01: trace flags (01 == sampled)
 *
 * If sampled, pg_tracing will generate spans for the current statement. A span
 * represents an operation with a start time, an end time and metadatas (block
 * stats, wal stats...).
 *
 * We will track the following operations:
 * - Top Span: The top span for a statement. They are created after extracting
 *   the traceid from traceparent or to represent a nested query.
 * - Planner: We track the time spent in the planner and report the planner counters.
 * - Executor: We only track steps of the Executor that could contain nested queries:
 *   Run and Finish
 *
 * A typical traced query will generate the following spans:
 * +------------------------------------------------------+
 * | Type: Select (top span)                              |
 * | Operation: Select * pgbench_accounts WHERE aid=$1;   |
 * +------------------------+---+-------------------------+
 * | Type: Planner          |   |Type: Executor        |
 * | Operation: Planner     |   |Operation: Run        |
 * +------------------------+   +----------------------+
 *
 * At the end of a statement, all generated spans are copied in the shared memory.
 * The spans are made available through the pg_tracing_peek_spans and
 * pg_tracing_consume_spans. Sending spans to a trace endpoint is outside the scope of
 * this module. It is expected that an external application will collect the spans
 * through the provided pg_tracing_consume_spans view and send them to a trace consumer.
 *
 * IDENTIFICATION
 *	  contrib/pg_tracing/pg_tracing.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "common/pg_prng.h"
#include "funcapi.h"
#include "nodes/extensible.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "pg_tracing.h"
#include "storage/ipc.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/varlena.h"
#include "utils/ruleutils.h"

PG_MODULE_MAGIC;

typedef enum
{
	PG_TRACING_TRACK_NONE,		/* track no statements */
	PG_TRACING_TRACK_TOP,		/* only top level statements */
	PG_TRACING_TRACK_ALL		/* all statements, including nested ones */
}			pgTracingTrackLevel;

typedef enum
{
	PG_TRACING_KEEP_ON_FULL,	/* Keep existing buffers when full */
	PG_TRACING_DROP_ON_FULL		/* Drop current buffers when full */
}			pgTracingBufferMode;

/*
 * Structure to store flexible array of spans
 */
typedef struct pgTracingSpans
{
	int			end;			/* Index of last element */
	int			max;			/* Maximum number of element */
	Span		spans[FLEXIBLE_ARRAY_MEMBER];
}			pgTracingSpans;

/*
 * Structure to store per exec level informations
 */
typedef struct pgTracingPerLevelBuffer
{
	uint64		query_id;		/* Query id by for this level when available */
	pgTracingSpans *top_spans;	/* top spans for the nested level */
	uint64		executor_run_span_id;	/* executor run span id for this
										 * level. Executor run is used as
										 * parent for spans generated from
										 * planstate */
	TimestampTz executor_start;
	TimestampTz executor_end;
}			pgTracingPerLevelBuffer;

/*
 * Structure to store Query id filtering array of query id used for filtering
 */
typedef struct pgTracingQueryIdFilter
{
	int			num_query_id;	/* number of query ids */
	uint64		query_ids[FLEXIBLE_ARRAY_MEMBER];
}			pgTracingQueryIdFilter;

/* GUC variables */
static int	pg_tracing_max_span;	/* Maximum number of spans to store */
static int	pg_tracing_max_parameter_str;	/* Maximum number of spans to
											 * store */
static bool pg_tracing_planstate_spans = true;	/* Generate spans from the
												 * execution plan */
static bool pg_tracing_deparse_plan = true; /* Deparse plan to generate more
											 * detailed spans */
static bool pg_tracing_trace_parallel_workers = true;	/* True to generate
														 * spans from parallel
														 * workers */
static double pg_tracing_sample_rate = 0;	/* Sample rate applied to queries
											 * without SQLCommenter */
static double pg_tracing_caller_sample_rate = 1;	/* Sample rate applied to
													 * queries with
													 * SQLCommenter */
static bool pg_tracing_export_parameters = true;	/* Export query's
													 * parameters as span
													 * metadata */
static int	pg_tracing_track = PG_TRACING_TRACK_ALL;	/* tracking level */
static bool pg_tracing_track_utility = true;	/* whether to track utility
												 * commands */
static int	pg_tracing_buffer_mode = PG_TRACING_KEEP_ON_FULL;	/* behaviour on full
																 * buffer */
static char *pg_tracing_filter_query_ids = NULL;	/* only sample query
													 * matching query ids */

static const struct config_enum_entry track_options[] =
{
	{"none", PG_TRACING_TRACK_NONE, false},
	{"top", PG_TRACING_TRACK_TOP, false},
	{"all", PG_TRACING_TRACK_ALL, false},
	{NULL, 0, false}
};

static const struct config_enum_entry buffer_mode_options[] =
{
	{"keep_on_full", PG_TRACING_KEEP_ON_FULL, false},
	{"drop_on_full", PG_TRACING_DROP_ON_FULL, false},
	{NULL, 0, false}
};

#define pg_tracking_level(level) \
	((pg_tracing_track == PG_TRACING_TRACK_ALL || \
	(pg_tracing_track == PG_TRACING_TRACK_TOP && (level) == 0)))

#define pg_tracing_enabled(trace_context, level) \
	(trace_context->traceparent.sampled && pg_tracking_level(level))

#define US_IN_S INT64CONST(1000000)
#define INT64_HEX_FORMAT "%016" INT64_MODIFIER "x"

PG_FUNCTION_INFO_V1(pg_tracing_info);
PG_FUNCTION_INFO_V1(pg_tracing_spans);
PG_FUNCTION_INFO_V1(pg_tracing_reset);

/*
 * Global variables
 */

/* Memory context for pg_tracing. */
MemoryContext pg_tracing_mem_ctx;

/* trace context at the root level of parse/planning hook */
static struct pgTracingTraceContext root_trace_context;

/* trace context used in nested levels or within executor hooks */
static struct pgTracingTraceContext current_trace_context;

/* Latest trace id observed */
static TraceId latest_trace_id;

/* Latest local transaction id traced */
static LocalTransactionId latest_lxid = InvalidLocalTransactionId;

/* Shared state with stats and file external state */
static pgTracingSharedState * pg_tracing = NULL;

/*
 * Shared buffer storing spans. Query with sampled flag will add new spans to the
 * shared state at the end of the traced query.
 * Those spans will be consumed during calls to pg_tracing_consume_spans.
 */
static pgTracingSpans * shared_spans = NULL;

/*
 * Store spans for the current trace.
 * They will be added to shared_spans at the end of the query tracing.
 */
static pgTracingSpans * current_trace_spans;

/*
* Text for spans are buffered in this stringinfo and written at
* the end of the query tracing in a single write.
*/
static StringInfo current_trace_text;

/*
 * Maximum nested level for a query to know how many top spans we need to
 * copy in shared_spans.
 */
static int	max_nested_level = -1;

/* Current nesting depth of planner calls */
static int	plan_nested_level = 0;

/* Current nesting depth of ExecutorRun+ProcessUtility calls */
int			exec_nested_level = 0;

/* Number of spans initially allocated at the start of a trace. */
static int	pg_tracing_initial_allocated_spans = 25;

static pgTracingPerLevelBuffer * per_level_buffers = NULL;
static pgTracingQueryIdFilter * query_id_filter = NULL;

static void pg_tracing_shmem_request(void);
static void pg_tracing_shmem_startup(void);

/* Saved hook values in case of unload */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

static void pg_tracing_post_parse_analyze(ParseState *pstate, Query *query,
										  JumbleState *jstate);
static PlannedStmt *pg_tracing_planner_hook(Query *parse,
											const char *query_string,
											int cursorOptions,
											ParamListInfo params);
static void pg_tracing_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pg_tracing_ExecutorRun(QueryDesc *queryDesc,
								   ScanDirection direction,
								   uint64 count, bool execute_once);
static void pg_tracing_ExecutorFinish(QueryDesc *queryDesc);
static void pg_tracing_ExecutorEnd(QueryDesc *queryDesc);
static void pg_tracing_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
									  bool readOnlyTree,
									  ProcessUtilityContext context,
									  ParamListInfo params,
									  QueryEnvironment *queryEnv,
									  DestReceiver *dest, QueryCompletion *qc);

static pgTracingStats get_empty_pg_tracing_stats(void);

static bool check_filter_query_ids(char **newval, void **extra, GucSource source);
static void assign_filter_query_ids(const char *newval, void *extra);

static void cleanup_tracing(void);

/*
 * Module load callback
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("pg_tracing.max_span",
							"Maximum number of spans stored in shared memory.",
							NULL,
							&pg_tracing_max_span,
							5000,
							0,
							500000,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_tracing.max_parameter_size",
							"Maximum size of parameters. -1 to disable parameter in top span.",
							NULL,
							&pg_tracing_max_parameter_str,
							1024,
							0,
							10000,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("pg_tracing.trace_parallel_workers",
							 "Whether to generate samples from parallel workers.",
							 NULL,
							 &pg_tracing_trace_parallel_workers,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_tracing.deparse_plan",
							 "Deparse query plan to generate more details on a plan node.",
							 NULL,
							 &pg_tracing_deparse_plan,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_tracing.planstate_spans",
							 "Generate spans from the executed plan.",
							 NULL,
							 &pg_tracing_planstate_spans,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("pg_tracing.track",
							 "Selects which statements are tracked by pg_tracing.",
							 NULL,
							 &pg_tracing_track,
							 PG_TRACING_TRACK_ALL,
							 track_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_tracing.track_utility",
							 "Selects whether utility commands are tracked by pg_tracing.",
							 NULL,
							 &pg_tracing_track_utility,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("pg_tracing.buffer_mode",
							 "Controls behaviour on full buffer.",
							 NULL,
							 &pg_tracing_buffer_mode,
							 PG_TRACING_KEEP_ON_FULL,
							 buffer_mode_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("pg_tracing.sample_rate",
							 "Fraction of queries without sampled flag or tracecontext to process.",
							 NULL,
							 &pg_tracing_sample_rate,
							 0.0,
							 0.0,
							 1.0,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("pg_tracing.caller_sample_rate",
							 "Fraction of queries having a tracecontext with sampled flag to process.",
							 NULL,
							 &pg_tracing_caller_sample_rate,
							 1.0,
							 0.0,
							 1.0,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomStringVariable("pg_tracing.filter_query_ids",
							   "Limiting sampling to the provided query ids.",
							   NULL,
							   &pg_tracing_filter_query_ids,
							   "",
							   PGC_USERSET,
							   GUC_LIST_INPUT,
							   check_filter_query_ids,
							   assign_filter_query_ids,
							   NULL);

	DefineCustomBoolVariable("pg_tracing.export_parameters",
							 "Export query's parameters as span metadata.",
							 NULL,
							 &pg_tracing_export_parameters,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	MarkGUCPrefixReserved("pg_tracing");

	/* For jumble state */
	EnableQueryId();

	/* Install hooks. */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pg_tracing_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pg_tracing_shmem_startup;

	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = pg_tracing_post_parse_analyze;

	prev_planner_hook = planner_hook;
	planner_hook = pg_tracing_planner_hook;

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pg_tracing_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pg_tracing_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pg_tracing_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pg_tracing_ExecutorEnd;

	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = pg_tracing_ProcessUtility;
}

/*
 * Estimate shared memory space needed.
 */
static Size
pg_tracing_memsize(void)
{
	Size		size;

	/* pg_tracing shared state */
	size = sizeof(pgTracingSharedState);
	/* span struct */
	size = add_size(size, sizeof(pgTracingSpans));
	/* the span variable array */
	size = add_size(size, mul_size(pg_tracing_max_span, sizeof(Span)));
	/* and the parallel workers context  */
	size = add_size(size, mul_size(max_parallel_workers, sizeof(pgTracingParallelContext)));

	return size;
}

/*
 * Parse query id for sampling filter
 */
static bool
check_filter_query_ids(char **newval, void **extra, GucSource source)
{
	char	   *rawstring;
	List	   *queryidlist;
	ListCell   *l;
	pgTracingQueryIdFilter *result;
	pgTracingQueryIdFilter *query_ids;
	int			num_query_ids = 0;
	size_t		size_query_id_filter;

	if (strcmp(*newval, "") == 0)
	{
		*extra = NULL;
		return true;
	}

	/* Need a modifiable copy of string */
	rawstring = pstrdup(*newval);

	if (!SplitIdentifierString(rawstring, ',', &queryidlist))
	{
		/* syntax error in list */
		GUC_check_errdetail("List syntax is invalid.");
		pfree(rawstring);
		list_free(queryidlist);
		return false;
	}

	size_query_id_filter = sizeof(pgTracingQueryIdFilter) + list_length(queryidlist) * sizeof(uint64);
	/* Work on a palloced buffer */
	query_ids = (pgTracingQueryIdFilter *) palloc(size_query_id_filter);

	foreach(l, queryidlist)
	{
		char	   *query_id_str = (char *) lfirst(l);
		int64		query_id = strtoi64(query_id_str, NULL, 10);

		if (errno == EINVAL || errno == ERANGE)
		{
			GUC_check_errdetail("Query id is not a valid int64: \"%s\".", query_id_str);
			pfree(rawstring);
			list_free(queryidlist);
			return false;
		}
		query_ids->query_ids[num_query_ids++] = query_id;
	}
	query_ids->num_query_id = num_query_ids;

	pfree(rawstring);
	list_free(queryidlist);

	/* Copy query id filter to a guc malloced result */
	result = (pgTracingQueryIdFilter *) guc_malloc(LOG, size_query_id_filter);
	if (result == NULL)
		return false;
	memcpy(result, query_ids, size_query_id_filter);

	*extra = result;
	return true;
}

/*
 * Assign parsed query id filter
 */
static void
assign_filter_query_ids(const char *newval, void *extra)
{
	query_id_filter = (pgTracingQueryIdFilter *) extra;
}

/*
 * shmem_request hook: request additional shared resources.  We'll allocate
 * or attach to the shared resources in pg_tracing_shmem_startup().
 */
static void
pg_tracing_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	RequestAddinShmemSpace(pg_tracing_memsize());
	RequestNamedLWLockTranche("pg_tracing", 1);
}

/*
 * Append a string current_trace_text StringInfo
 */
int
add_str_to_trace_buffer(const char *str, int str_len)
{
	int			position = current_trace_text->cursor;

	Assert(str_len > 0);

	appendBinaryStringInfo(current_trace_text, str, str_len);
	appendStringInfoChar(current_trace_text, '\0');
	current_trace_text->cursor = current_trace_text->len;
	return position;
}


/*
 * Add the worker name to the provided stringinfo
 */
static int
add_worker_name_to_trace_buffer(StringInfo str_info, int parallel_worker_number)
{
	int			position = str_info->cursor;

	appendStringInfo(str_info, "Worker %d", parallel_worker_number);
	appendStringInfoChar(str_info, '\0');
	str_info->cursor = str_info->len;
	return position;
}

/*
 * Store a span in the current_trace_spans buffer
 */
void
store_span(const Span * span)
{
	Assert(span->ended);
	Assert(span->span_id > 0);

	if (current_trace_spans->end >= current_trace_spans->max)
	{
		MemoryContext oldcxt;

		/* Need to extend. */
		current_trace_spans->max *= 2;
		oldcxt = MemoryContextSwitchTo(pg_tracing_mem_ctx);
		current_trace_spans = repalloc(current_trace_spans, sizeof(pgTracingSpans) +
									   current_trace_spans->max * sizeof(Span));
		MemoryContextSwitchTo(oldcxt);
	}
	current_trace_spans->spans[current_trace_spans->end++] = *span;
}

/*
 * Get the latest span for a specific level. The span must exists
 */
static Span *
get_latest_top_span(int nested_level)
{
	pgTracingSpans *top_spans;

	Assert(nested_level <= max_nested_level);
	Assert(nested_level >= 0);
	top_spans = per_level_buffers[nested_level].top_spans;
	Assert(top_spans->end > 0);
	return &top_spans->spans[top_spans->end - 1];
}

/*
 * Create a new top_span for the current exec nested level
 */
static Span *
allocate_new_top_span(void)
{
	pgTracingSpans *top_spans;
	Span	   *top_span;

	top_spans = per_level_buffers[exec_nested_level].top_spans;
	if (top_spans->end >= top_spans->max)
	{
		MemoryContext oldcxt;
		int			old_spans_max = top_spans->max;

		top_spans->max *= 2;
		oldcxt = MemoryContextSwitchTo(pg_tracing_mem_ctx);
		top_spans = repalloc0(top_spans,
							  sizeof(pgTracingSpans) + old_spans_max * sizeof(Span),
							  sizeof(pgTracingSpans) + old_spans_max * 2 * sizeof(Span));
		per_level_buffers[exec_nested_level].top_spans = top_spans;
		MemoryContextSwitchTo(oldcxt);
	}
	top_span = &top_spans->spans[top_spans->end++];
	Assert(top_span->span_id == 0);
	return top_span;
}

/*
 * Drop the latest top span for the current nested level
 */
static void
pop_top_span(void)
{
	pgTracingSpans *top_spans = per_level_buffers[exec_nested_level].top_spans;

	Assert(top_spans->end > 0);
	/* Reset span id of the discarded span since it could be reused */
	top_spans->spans[--top_spans->end].span_id = 0;
}

/*
 * End the latest top span for the current nested level.
 * If pop_span is true, store the span in the current_trace_spans and remove it
 * from the per level buffers.
 */
static void
end_latest_top_span(const TimestampTz *end_time, bool pop_span)
{
	Span	   *top_span;

	/* Check if the level was allocated */
	if (exec_nested_level > max_nested_level)
		return;

	top_span = get_latest_top_span(exec_nested_level);
	end_span(top_span, end_time);

	/* Store span and remove it from the per_level_buffers */
	if (pop_span)
	{
		store_span(top_span);
		/* Restore previous top span */
		pop_top_span();
	}
}

/*
 * End all spans for the current nested level
 */
static TimestampTz
end_nested_level(void)
{
	TimestampTz span_end_time = GetCurrentTimestamp();
	pgTracingSpans *top_spans;

	if (exec_nested_level > max_nested_level)
		/* No nested level were created */
		goto finish;

	top_spans = per_level_buffers[exec_nested_level].top_spans;
	if (top_spans->end == 0)
		/* No top spans to end */
		goto finish;

	for (int i = 0; i < top_spans->end; i++)
	{
		Span	   *top_span = top_spans->spans + i;

		if (!top_span->ended)
			end_span(top_span, &span_end_time);
	}

finish:
	exec_nested_level--;
	return span_end_time;
}

/*
 * Drop all spans from the shared buffer and truncate the query file.
 * Exclusive lock on pg_tracing->lock should be acquired beforehand.
 */
static void
drop_all_spans_locked(void)
{
	/* Drop all spans */
	shared_spans->end = 0;

	/* Reset query file position */
	pg_tracing->extent = 0;
	pg_truncate(PG_TRACING_TEXT_FILE, 0);
}

/*
 * Check if we still have available space in the shared spans.
 *
 * Between the moment we check and the moment we insert spans, the buffer may be
 * full but we will redo a check before. This check is done when starting a top
 * span to bail out early if the buffer is already full.
 */
static bool
check_full_shared_spans(void)
{
	bool		full_buffer = false;

	LWLockAcquire(pg_tracing->lock, LW_SHARED);
	full_buffer = shared_spans->end >= shared_spans->max;
	if (full_buffer && pg_tracing_buffer_mode != PG_TRACING_DROP_ON_FULL)
	{
		/*
		 * Buffer is full and we want to keep existing spans. Drop the new
		 * trace.
		 */
		pg_tracing->stats.dropped_traces++;
		LWLockRelease(pg_tracing->lock);
		return full_buffer;
	}
	LWLockRelease(pg_tracing->lock);

	if (!full_buffer)
		/* We have room in the shared buffer */
		return full_buffer;

	/*
	 * We have a full buffer and we want to drop everything, get an exclusive
	 * lock
	 */
	LWLockAcquire(pg_tracing->lock, LW_EXCLUSIVE);
	/* Recheck for full buffer */
	full_buffer = shared_spans->end >= shared_spans->max;
	if (full_buffer)
	{
		pg_tracing->stats.dropped_spans += shared_spans->end;
		drop_all_spans_locked();
	}
	LWLockRelease(pg_tracing->lock);

	return full_buffer;
}

/*
 * Reset trace_context fields
 */
static void
reset_trace_context(pgTracingTraceContext * trace_context)
{
	trace_context->traceparent.sampled = 0;
	trace_context->traceparent.trace_id.traceid_right = 0;
	trace_context->traceparent.trace_id.traceid_left = 0;
	trace_context->traceparent.parent_id = 0;
	trace_context->root_span.span_id = 0;
}

/*
 * shmem_startup hook: allocate or attach to shared memory, Also create and
 * load the query-texts file, which is expected to exist (even if empty)
 * while the module is enabled.
 */
static void
pg_tracing_shmem_startup(void)
{
	bool		found_pg_tracing;
	bool		found_shared_spans;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	pg_tracing = NULL;

	/* Create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	reset_trace_context(&root_trace_context);
	reset_trace_context(&current_trace_context);
	pg_tracing = ShmemInitStruct("PgTracing Shared", sizeof(pgTracingSharedState),
								 &found_pg_tracing);
	shared_spans = ShmemInitStruct("PgTracing Spans",
								   sizeof(pgTracingSpans) +
								   pg_tracing_max_span * sizeof(Span),
								   &found_shared_spans);

	/* Initialize pg_tracing memory context */
	pg_tracing_mem_ctx = AllocSetContextCreate(TopMemoryContext,
											   "pg_tracing memory context",
											   ALLOCSET_DEFAULT_SIZES);

	/* Initialize shmem for trace propagation to parallel workers */
	pg_tracing_shmem_parallel_startup();

	/* First time, let's init shared state */
	if (!found_pg_tracing)
	{
		pg_tracing->stats = get_empty_pg_tracing_stats();
		pg_tracing->lock = &(GetNamedLWLockTranche("pg_tracing"))->lock;
	}
	if (!found_shared_spans)
	{
		shared_spans->end = 0;
		shared_spans->max = pg_tracing_max_span;
	}
	LWLockRelease(AddinShmemInitLock);
}

/*
 * Process a query descriptor: Gather all query instrumentation in the query
 * span counters
 */
static void
process_query_desc(pgTracingTraceContext * trace_context, const QueryDesc *queryDesc,
				   int sql_error_code, TimestampTz parent_end)
{
	NodeCounters *node_counters = &get_latest_top_span(exec_nested_level)->node_counters;

	/* Process total counters */
	if (queryDesc->totaltime)
	{
		InstrEndLoop(queryDesc->totaltime);
		node_counters->buffer_usage = queryDesc->totaltime->bufusage;
		node_counters->wal_usage = queryDesc->totaltime->walusage;
	}
	/* Process JIT counter */
	if (queryDesc->estate->es_jit)
		node_counters->jit_usage = queryDesc->estate->es_jit->instr;
	node_counters->rows = queryDesc->estate->es_total_processed;


	/* Process planstate */
	if (queryDesc->planstate && queryDesc->planstate->instrument != NULL && pg_tracing_planstate_spans)
	{
		Bitmapset  *rels_used = NULL;
		planstateTraceContext planstateTraceContext;
		TimestampTz latest_end = 0;
		uint64		parent_id = per_level_buffers[exec_nested_level].executor_run_span_id;
		uint64		query_id = per_level_buffers[exec_nested_level].query_id;
		TimestampTz parent_start = per_level_buffers[exec_nested_level].executor_start;

		planstateTraceContext.rtable_names = select_rtable_names_for_explain(queryDesc->plannedstmt->rtable, rels_used);
		planstateTraceContext.trace_id = trace_context->traceparent.trace_id;
		planstateTraceContext.ancestors = NULL;
		planstateTraceContext.sql_error_code = sql_error_code;
		/* Prepare the planstate context for deparsing */
		planstateTraceContext.deparse_ctx = NULL;
		if (pg_tracing_deparse_plan)
			planstateTraceContext.deparse_ctx =
				deparse_context_for_plan_tree(queryDesc->plannedstmt,
											  planstateTraceContext.rtable_names);

		generate_span_from_planstate(queryDesc->planstate, &planstateTraceContext,
									 parent_id, query_id, parent_start, parent_end, &latest_end);
	}
}

/*
 * Create a trace id for the trace context if there's none.
 * If trace was started from the global sample rate without a parent trace, we
 * need to generate a random trace id.
 */
static void
set_trace_id(struct pgTracingTraceContext *trace_context)
{
	LocalTransactionId current_lxid;

#if PG_VERSION_NUM >= 170000
	current_lxid = MyProc->vxid.lxid;
#else
	current_lxid = MyProc->lxid;
#endif

	if (!traceid_zero(trace_context->traceparent.trace_id))
	{
		/* Update last lxid seen */
		latest_lxid = current_lxid;
		return;
	}

	/*
	 * We want to keep the same trace id for all statements within the same
	 * transaction. For that, we check if we're in the same local xid.
	 */
	if (current_lxid == latest_lxid)
	{
		trace_context->traceparent.trace_id = latest_trace_id;
		return;
	}

	/*
	 * We leave parent_id to 0 as a way to indicate that this is a standalone
	 * trace.
	 */
	Assert(trace_context->traceparent.parent_id == 0);

	trace_context->traceparent.trace_id.traceid_left = pg_prng_int64(&pg_global_prng_state);
	trace_context->traceparent.trace_id.traceid_right = pg_prng_int64(&pg_global_prng_state);
	latest_trace_id = trace_context->traceparent.trace_id;
	latest_lxid = current_lxid;
}

/*
 * Check whether trace context is filtered based on query id
 */
static bool
is_query_id_filtered(const struct pgTracingTraceContext *trace_context)
{
	/* No query id filter defined */
	if (query_id_filter == NULL)
		return false;
	for (int i = 0; i < query_id_filter->num_query_id; i++)
		if (query_id_filter->query_ids[i] == trace_context->query_id)
			return false;
	return true;
}

/*
 * Decide whether a query should be sampled depending on the traceparent sampled
 * flag and the provided sample rate configurations
 */
static bool
is_query_sampled(const struct pgTracingTraceContext *trace_context)
{
	double		rand;
	bool		sampled;

	/* Everything is sampled */
	if (pg_tracing_sample_rate >= 1.0)
		return true;

	/* No SQLCommenter sampled and no global sample rate */
	if (!trace_context->traceparent.sampled && pg_tracing_sample_rate == 0.0)
		return false;

	/* SQLCommenter sampled and caller sample is on */
	if (trace_context->traceparent.sampled && pg_tracing_caller_sample_rate >= 1.0)
		return true;

	/*
	 * We have a non zero global sample rate or a sampled flag. Either way, we
	 * need a rand value
	 */
	rand = pg_prng_double(&pg_global_prng_state);
	if (trace_context->traceparent.sampled)
		/* Sampled flag case */
		sampled = (rand < pg_tracing_caller_sample_rate);
	else
		/* Global rate case */
		sampled = (rand < pg_tracing_sample_rate);
	return sampled;
}

/*
 * Extract trace_context from the query text.
 * Sampling rate will be applied to the trace_context.
 */
static void
extract_trace_context(struct pgTracingTraceContext *trace_context, ParseState *pstate,
					  uint64 query_id)
{
	/* Timestamp of the latest statement checked for sampling. */
	static TimestampTz last_statement_check_for_sampling = 0;
	TimestampTz statement_start_ts;

	/* Safety check... */
	if (!pg_tracing || !pg_tracking_level(exec_nested_level))
		return;

	/* sampling already started */
	if (trace_context->traceparent.sampled)
		return;

	/* Don't start tracing if we're not at the root level */
	if (exec_nested_level > 0)
		return;

	/* Both sampling rate are set to 0, no tracing will happen */
	if (pg_tracing_sample_rate == 0 && pg_tracing_caller_sample_rate == 0)
		return;

	/*
	 * In a parallel worker, check the parallel context shared buffer to see
	 * if the leader left a trace context
	 */
	if (IsParallelWorker())
	{
		if (pg_tracing_trace_parallel_workers)
			fetch_parallel_context(trace_context);
		return;
	}

	Assert(trace_context->root_span.span_id == 0);
	Assert(traceid_zero(trace_context->traceparent.trace_id));

	if (pstate != NULL)
		extract_trace_context_from_query(trace_context, pstate->p_sourcetext);

	/*
	 * A statement can go through this function 3 times: post parsing, planner
	 * and executor run. If no sampling flag was extracted from SQLCommenter
	 * and we've already seen this statement (using statement_start ts), don't
	 * try to apply sample rate and exit.
	 */
	statement_start_ts = GetCurrentStatementStartTimestamp();
	if (trace_context->traceparent.sampled == 0
		&& last_statement_check_for_sampling == statement_start_ts)
		return;
	/* First time we see this statement, save the time */
	last_statement_check_for_sampling = statement_start_ts;

	trace_context->query_id = query_id;
	if (is_query_id_filtered(trace_context))
		/* Query id filter is not matching, disable sampling */
		trace_context->traceparent.sampled = 0;
	else
		trace_context->traceparent.sampled = is_query_sampled(trace_context);

	if (trace_context->traceparent.sampled && check_full_shared_spans())
		/* Buffer is full, abort sampling */
		trace_context->traceparent.sampled = 0;

	if (trace_context->traceparent.sampled)
		set_trace_id(trace_context);
	else
		/* No sampling, reset the context */
		reset_trace_context(trace_context);
}

/*
 * Reset pg_tracing memory context and global state.
 */
static void
cleanup_tracing(void)
{
	if (!root_trace_context.traceparent.sampled &&
		!current_trace_context.traceparent.sampled)
		/* No need for cleaning */
		return;
	if (pg_tracing_trace_parallel_workers)
		remove_parallel_context();
	MemoryContextReset(pg_tracing_mem_ctx);
	reset_trace_context(&root_trace_context);
	reset_trace_context(&current_trace_context);
	max_nested_level = -1;
	current_trace_spans = NULL;
	per_level_buffers = NULL;
	cleanup_planstarts();
}

/*
 * Add span to the shared memory.
 * Exclusive lock on pg_tracing->lock must be acquired beforehand.
 */
static void
add_span_to_shared_buffer_locked(const Span * span)
{
	/* Spans must be ended before adding them to the shared buffer */
	Assert(span->ended);
	if (shared_spans->end >= shared_spans->max)
		pg_tracing->stats.dropped_spans++;
	else
	{
		pg_tracing->stats.processed_spans++;
		shared_spans->spans[shared_spans->end++] = *span;
	}
}

/*
 * End the query tracing and dump all spans in the shared buffer if we are at
 * the root level. This may happen either when query is finished or on a caught error.
 */
static void
end_tracing(pgTracingTraceContext * trace_context, Span * ongoing_span)
{
	Size		file_position = 0;

	/* We're still a nested query, tracing is not finished */
	if (exec_nested_level + plan_nested_level > 0)
		return;

	LWLockAcquire(pg_tracing->lock, LW_EXCLUSIVE);

	if (current_trace_text->len > 0)
	{
		/* Dump all buffered texts in file */
		text_store_file(pg_tracing, current_trace_text->data, current_trace_text->len,
						&file_position);

		/* Adjust file position of spans */
		for (int i = 0; i < current_trace_spans->end; i++)
			adjust_file_offset(current_trace_spans->spans + i, file_position);
		for (int i = 0; i <= max_nested_level; i++)
			for (int j = 0; j < per_level_buffers[i].top_spans->end; j++)
				adjust_file_offset(per_level_buffers[i].top_spans->spans + j,
								   file_position);
		if (ongoing_span != NULL)
			adjust_file_offset(ongoing_span, file_position);
	}

	/* We're at the end, add all stored spans to the shared memory */
	for (int i = 0; i < current_trace_spans->end; i++)
		add_span_to_shared_buffer_locked(&current_trace_spans->spans[i]);
	/* And all top spans that were not pushed to current_trace_spans */
	for (int i = 0; i <= max_nested_level; i++)
		for (int j = 0; j < per_level_buffers[i].top_spans->end; j++)
			add_span_to_shared_buffer_locked(&per_level_buffers[i].top_spans->spans[j]);

	/*
	 * We may have an ongoing span that was not allocated in
	 * current_trace_spans nor per_level_buffers, add it to the shared spans
	 */
	if (ongoing_span != NULL)
		add_span_to_shared_buffer_locked(ongoing_span);

	/* Update our stats with the new trace */
	pg_tracing->stats.processed_traces++;
	LWLockRelease(pg_tracing->lock);

	/* We can reset the memory context here */
	cleanup_tracing();
}

/*
 * When we catch an error (timeout, cancel query), we need to flag the ongoing
 * span with an error, send current spans in the shared buffer and clean our
 * memory context.
 *
 * We provide the ongoing span where the error was caught to attach the sql
 * error code to it.
 */
static void
handle_pg_error(pgTracingTraceContext * trace_context, Span * ongoing_span,
				const QueryDesc *queryDesc,
				TimestampTz span_end_time)
{
	int			sql_error_code;

	/* If we're not sampling the query, bail out */
	if (!pg_tracing_enabled(trace_context, exec_nested_level))
		return;

	sql_error_code = geterrcode();

	if (queryDesc != NULL)
		process_query_desc(trace_context, queryDesc, sql_error_code, span_end_time);

	/* End all ongoing top spans */
	for (int i = 0; i <= max_nested_level; i++)
	{
		pgTracingSpans *top_spans = per_level_buffers[i].top_spans;

		for (int j = 0; j < top_spans->end; j++)
		{
			Span	   *span = &top_spans->spans[j];

			if (!span->ended)
			{
				/* Assign the error code to the latest top span */
				span->sql_error_code = sql_error_code;
				end_span(span, &span_end_time);
			}
		}
	}

	if (ongoing_span != NULL)
	{
		ongoing_span->sql_error_code = sql_error_code;
		if (!ongoing_span->ended)
			end_span(ongoing_span, &span_end_time);
	}

	end_tracing(trace_context, ongoing_span);
}

/*
 * If we enter a new nested level, initialize all necessary buffers.
 */
static void
initialize_trace_level(void)
{
	/* Number of allocated levels. */
	static int	allocated_nested_level = 0;

	Assert(exec_nested_level >= 0);

	/* Check if we've already created a top span for this nested level */
	if (exec_nested_level <= max_nested_level)
		return;

	/* First time */
	if (max_nested_level == -1)
	{
		MemoryContext oldcxt;

		Assert(pg_tracing_mem_ctx->isReset);

		oldcxt = MemoryContextSwitchTo(pg_tracing_mem_ctx);
		/* initial allocation */
		allocated_nested_level = 1;
		per_level_buffers = palloc0(allocated_nested_level *
									sizeof(pgTracingPerLevelBuffer));
		for (int i = 0; i < allocated_nested_level; i++)
		{
			per_level_buffers[i].top_spans = palloc0(sizeof(pgTracingSpans) +
													 sizeof(Span));
			per_level_buffers[i].top_spans->max = 1;
		}
		current_trace_spans = palloc0(sizeof(pgTracingSpans) +
									  pg_tracing_initial_allocated_spans * sizeof(Span));
		current_trace_spans->max = pg_tracing_initial_allocated_spans;
		current_trace_text = makeStringInfo();

		MemoryContextSwitchTo(oldcxt);
	}
	else if (exec_nested_level >= allocated_nested_level)
	{
		/* New nested level, allocate more memory */
		MemoryContext oldcxt;
		int			old_allocated_nested_level = allocated_nested_level;

		oldcxt = MemoryContextSwitchTo(pg_tracing_mem_ctx);
		allocated_nested_level++;
		per_level_buffers = repalloc0(per_level_buffers, old_allocated_nested_level * sizeof(pgTracingPerLevelBuffer),
									  allocated_nested_level * sizeof(pgTracingPerLevelBuffer));
		for (int i = old_allocated_nested_level; i < allocated_nested_level; i++)
		{
			per_level_buffers[i].top_spans = palloc0(sizeof(pgTracingSpans) +
													 sizeof(Span));
			per_level_buffers[i].top_spans->max = 1;
		}
		MemoryContextSwitchTo(oldcxt);
	}
	max_nested_level = exec_nested_level;
}

/*
 * Start a new top span if we've entered a new nested level or if the previous
 * span at the same level ended.
 */
static void
begin_top_span(pgTracingTraceContext * trace_context, Span * top_span,
			   CmdType commandType, const Query *query, const JumbleState *jstate,
			   const PlannedStmt *pstmt, const char *query_text, TimestampTz start_time)
{
	int			query_len;
	const char *normalised_query;
	uint64		parent_id;

	/* in case of a cached plan, query might be unavailable */
	if (query != NULL)
		per_level_buffers[exec_nested_level].query_id = query->queryId;
	else if (trace_context->query_id > 0)
		per_level_buffers[exec_nested_level].query_id = trace_context->query_id;

	if (exec_nested_level <= 0)
		/* Root top span, use the parent id from the trace context */
		parent_id = trace_context->traceparent.parent_id;
	else
	{
		TracedPlanstate *parent_traced_planstate = get_parent_traced_planstate(exec_nested_level);
		Span	   *latest_top_span = get_latest_top_span(exec_nested_level - 1);

		/*
		 * Both planstate and previous top span can be the parent for the new
		 * top span, we use the most recent as a parent
		 */
		if (parent_traced_planstate != NULL && parent_traced_planstate->node_start >= latest_top_span->start)
			parent_id = parent_traced_planstate->span_id;
		else
			parent_id = latest_top_span->span_id;
	}

	begin_span(trace_context->traceparent.trace_id, top_span,
			   command_type_to_span_type(commandType),
			   NULL, parent_id,
			   per_level_buffers[exec_nested_level].query_id, &start_time);

	if (IsParallelWorker())
	{
		/*
		 * In a parallel worker, we use the worker name as the span's
		 * operation
		 */
		top_span->operation_name_offset = add_worker_name_to_trace_buffer(current_trace_text, ParallelWorkerNumber);
		return;
	}

	if (jstate && jstate->clocations_count > 0 && query != NULL)
	{
		/* jstate is available, normalise query and extract parameters' values */
		char	   *param_str;
		int			param_len;

		query_len = query->stmt_len;
		normalised_query = normalise_query_parameters(jstate, query_text,
													  query->stmt_location, &query_len,
													  &param_str, &param_len);
		Assert(param_len > 0);
		if (pg_tracing_export_parameters)
			top_span->parameter_offset = add_str_to_trace_buffer(param_str, param_len);
	}
	else
	{
		/*
		 * No jstate available, normalise query but we won't be able to
		 * extract parameters
		 */
		int			stmt_location;

		if (query != NULL && query->stmt_len > 0)
		{
			query_len = query->stmt_len;
			stmt_location = query->stmt_location;
		}
		else if (pstmt != NULL && pstmt->stmt_location != -1 && pstmt->stmt_len > 0)
		{
			query_len = pstmt->stmt_len;
			stmt_location = pstmt->stmt_location;
		}
		else
		{
			query_len = strlen(query_text);
			stmt_location = 0;
		}
		normalised_query = normalise_query(query_text, stmt_location, &query_len);
	}
	top_span->operation_name_offset = add_str_to_trace_buffer(normalised_query,
															  query_len);
}

/*
 * Get the ongoing top span if it exists or create it
 */
static Span *
get_or_allocate_top_span(pgTracingTraceContext * trace_context, bool in_parse_or_plan)
{
	Span	   *span;

	if (in_parse_or_plan && exec_nested_level == 0)

		/*
		 * A the root post parse and plan, we want to use trace_context's
		 * root_span as the top span in per_level_buffers might still be
		 * ongoing.
		 */
		return &trace_context->root_span;

	if (per_level_buffers[exec_nested_level].top_spans->end == 0)
		/* No spans were created in this level, allocate a new one */
		span = allocate_new_top_span();
	else
		span = get_latest_top_span(exec_nested_level);

	if (exec_nested_level == 0)

		/*
		 * At root level and outside of parse/plan hook, we need to copy the
		 * root span content
		 */
		*span = trace_context->root_span;

	return span;
}

/*
 * Initialise buffers if we are in a new nested level and start associated top span.
 * If the top span already exists for the current nested level, this has no effect.
 *
 * This needs to be called every time a top span could be started: post parse,
 * planner, executor start and process utility
 */
static uint64
initialize_top_span(pgTracingTraceContext * trace_context, CmdType commandType,
					Query *query, JumbleState *jstate, const PlannedStmt *pstmt,
					const char *query_text, TimestampTz start_time,
					bool in_parse_or_plan)
{
	Span	   *top_span;

	/* Make sure we have allocated the level */
	initialize_trace_level();

	top_span = get_or_allocate_top_span(trace_context, in_parse_or_plan);

	/* If the top_span is still ongoing, use it as it is */
	if (top_span->span_id > 0 && top_span->ended == false)
		return top_span->span_id;

	/* current_trace_spans buffer should have been allocated */
	Assert(current_trace_spans != NULL);

	if (top_span->span_id > 0)
	{
		/* The previous top span was closed, create a new one */
		Assert(top_span->ended);
		top_span = allocate_new_top_span();
	}

	/* This is a new top span, start it */
	begin_top_span(trace_context, top_span, commandType, query, jstate, pstmt,
				   query_text, start_time);
	return top_span->span_id;
}

/*
 * Post-parse-analyze hook
 *
 * Tracing can be started here if:
 * - The query has a SQLCommenter with traceparent parameter with sampled flag
 *   on and passes the caller_sample_rate
 * - The query passes the sample_rate
 *
 * If query is sampled, start the top span
 */
static void
pg_tracing_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate)
{
	TimestampTz start_top_span;
	pgTracingTraceContext *trace_context = &root_trace_context;
	bool		new_lxid;
#if PG_VERSION_NUM >= 170000
	new_lxid = MyProc->vxid.lxid != latest_lxid;
#else
	new_lxid = MyProc->lxid != latest_lxid;
#endif


	if (!pg_tracing_mem_ctx->isReset
		&& new_lxid
		&& exec_nested_level + plan_nested_level == 0)

		/*
		 * Some errors can happen outside of our PG_TRY (incorrect number of
		 * bind parameters for example) which will leave a dirty trace
		 * context. We can also have sampled individual Parse command through
		 * extended protocol. In this case, we just drop the spans and reset
		 * memory context
		 */
		cleanup_tracing();

	if (exec_nested_level + plan_nested_level == 0)

		/*
		 * At the root level, clean any leftover state of previous trace
		 * context
		 */
		reset_trace_context(&root_trace_context);
	else
		/* We're in a nested query, grab the ongoing trace_context */
		trace_context = &current_trace_context;

	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query, jstate);

	/* If disabled, don't trace utility statement */
	if (query->utilityStmt && !pg_tracing_track_utility)
		return;

	/* Evaluate if query is sampled or not */
	extract_trace_context(trace_context, pstate, query->queryId);

	if (!trace_context->traceparent.sampled)
		/* Query is not sampled, nothing to do */
		return;

	/*
	 * We want to avoid calling get_ns at the start of post parse as it will
	 * impact all queries and we will only use it when the query is sampled.
	 */
	start_top_span = GetCurrentTimestamp();

	/*
	 * Either we're inside a nested sampled query or we've parsed a query with
	 * the sampled flag, start a new level with a top span
	 */
	initialize_top_span(trace_context, query->commandType,
						query, jstate, NULL,
						pstate->p_sourcetext, start_top_span,
						true);
}

/*
 * Planner hook.
 * Tracing can start here if the query skipped parsing (prepared statement) and
 * passes the random sample_rate. If query is sampled, create a plan span and
 * start the top span if not already started.
 */
static PlannedStmt *
pg_tracing_planner_hook(Query *query, const char *query_string, int cursorOptions,
						ParamListInfo params)
{
	PlannedStmt *result;
	uint64		parent_id;
	Span	   *span_planner;
	pgTracingTraceContext *trace_context = &root_trace_context;
	TimestampTz start_span_time;

	if (exec_nested_level > 0)
		/* We're in a nested query, grab the ongoing trace_context */
		trace_context = &current_trace_context;

	/* Evaluate if query is sampled or not */
	extract_trace_context(trace_context, NULL, query->queryId);

	if (!pg_tracing_enabled(trace_context, plan_nested_level + exec_nested_level))
	{
		/* No sampling */
		if (prev_planner_hook)
			result = prev_planner_hook(query, query_string, cursorOptions,
									   params);
		else
			result = standard_planner(query, query_string, cursorOptions,
									  params);
		return result;
	}

	start_span_time = GetCurrentTimestamp();

	if (plan_nested_level == 0)

		/*
		 * We may have skipped parsing if statement was prepared, create a new
		 * top span in this case.
		 */
		parent_id = initialize_top_span(trace_context, query->commandType, query,
										NULL, NULL, query_string, start_span_time, true);
	else
		/* We're in a nested plan, grab the latest top span */
		parent_id = get_latest_top_span(exec_nested_level)->span_id;

	/* Create and start the planner span */
	span_planner = allocate_new_top_span();
	begin_span(trace_context->traceparent.trace_id, span_planner, SPAN_PLANNER,
			   NULL, parent_id,
			   per_level_buffers[exec_nested_level].query_id, &start_span_time);

	plan_nested_level++;
	PG_TRY();
	{
		if (prev_planner_hook)
			result = prev_planner_hook(query, query_string, cursorOptions, params);
		else
			result = standard_planner(query, query_string, cursorOptions, params);
	}
	PG_CATCH();
	{
		TimestampTz span_end_time = GetCurrentTimestamp();
		Span	   *root_span = NULL;

		plan_nested_level--;
		span_planner->sql_error_code = geterrcode();
		if (exec_nested_level == 0)
		{
			/*
			 * The root span at level 0 is stored in trace_context. Ideally,
			 * we should copy it to a top span created with
			 * allocate_new_top_span(). However, since we are within a catch
			 * block that could be due to a out of memory error, we want to
			 * avoid new allocations. We can just pass the existing span to be
			 * copied in the shared memory
			 */
			root_span = &trace_context->root_span;
		}
		handle_pg_error(trace_context, root_span, NULL, span_end_time);
		PG_RE_THROW();
	}
	PG_END_TRY();
	plan_nested_level--;

	/* End planner span */
	end_latest_top_span(NULL, true);

	/* If we have a prepared statement, add bound parameters to the top span */
	if (params != NULL && pg_tracing_export_parameters)
	{
		char	   *paramStr = BuildParamLogString(params, NULL,
												   pg_tracing_max_parameter_str);

		if (paramStr != NULL)
		{
			Span	   *top_span;

			if (exec_nested_level == 0)
				/* The root span at level 0 is stored in trace_context, use it */
				top_span = &trace_context->root_span;
			else
				top_span = get_latest_top_span(exec_nested_level);
			top_span->parameter_offset = add_str_to_trace_buffer(paramStr,
																 strlen(paramStr));
		}
	}
	return result;
}

/*
 * ExecutorStart hook: Activate query instrumentation if query is sampled.
 * Tracing can be started here if the query used a cached plan and passes the
 * random sample_rate.
 * If query is sampled, start the top span if it doesn't already exist.
 */
static void
pg_tracing_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	pgTracingTraceContext *trace_context = &current_trace_context;
	bool		executor_sampled;
	bool		is_lazy_function;

	if (exec_nested_level + plan_nested_level == 0)
	{
		/* We're at the root level, copy trace context from parsing/planning */
		*trace_context = root_trace_context;
	}

	/* Evaluate if query is sampled or not */
	extract_trace_context(trace_context, NULL, queryDesc->plannedstmt->queryId);

	/*
	 * We can detect the presence of lazy function through the node tag and
	 * the executor flags. Lazy function will go through an ExecutorRun with
	 * every call, possibly generating thousands of spans (ex: lazy call of
	 * generate_series) which is not manageable and not very useful. If lazy
	 * functions are detected, we don't generate spans.
	 */
	is_lazy_function = nodeTag(queryDesc->plannedstmt->planTree) == T_FunctionScan
		&& eflags == EXEC_FLAG_SKIP_TRIGGERS;
	executor_sampled = pg_tracing_enabled(trace_context, exec_nested_level)
		&& !is_lazy_function;

	if (executor_sampled)
	{
		TimestampTz start_span_time = GetCurrentTimestamp();

		/*
		 * In case of a cached plan, we haven't gone through neither parsing
		 * nor planner hook. Create the top case in this case.
		 */
		initialize_top_span(trace_context, queryDesc->operation, NULL, NULL, NULL,
							queryDesc->sourceText, start_span_time, false);

		/*
		 * We only need full instrumenation if we generate spans from
		 * planstate
		 */
		if (pg_tracing_planstate_spans)
			queryDesc->instrument_options = INSTRUMENT_ALL;
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	/* Allocate totaltime instrumentation in the per-query context */
	if (executor_sampled && queryDesc->totaltime == NULL)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
		queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL, false);
		MemoryContextSwitchTo(oldcxt);
	}
}

/*
 * ExecutorRun hook: track nesting depth and create ExecutorRun span.
 * ExecutorRun can create nested queries so we need to create ExecutorRun span
 * as a top span.
 * If the plan needs to create parallel workers, push the trace context in the parallel shared buffer.
 */
static void
pg_tracing_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
					   bool execute_once)
{
	pgTracingTraceContext *trace_context = &current_trace_context;
	TimestampTz span_end_time;
	bool		executor_sampled = pg_tracing_enabled(trace_context, exec_nested_level) && queryDesc->totaltime != NULL;

	if (executor_sampled)
	{
		Span	   *executor_run_span;
		TimestampTz span_start_time = GetCurrentTimestamp();
		uint64		parent_id = get_latest_top_span(exec_nested_level)->span_id;

		/* Start ExecutorRun span as a new top span */
		executor_run_span = allocate_new_top_span();
		begin_span(trace_context->traceparent.trace_id, executor_run_span,
				   SPAN_EXECUTOR_RUN, NULL, parent_id,
				   per_level_buffers[exec_nested_level].query_id, &span_start_time);
		per_level_buffers[exec_nested_level].executor_run_span_id = executor_run_span->span_id;
		per_level_buffers[exec_nested_level].executor_start = executor_run_span->start;

		/*
		 * If this query starts parallel worker, push the trace context for
		 * the child processes
		 */
		if (queryDesc->plannedstmt->parallelModeNeeded && pg_tracing_trace_parallel_workers)
			add_parallel_context(trace_context, executor_run_span->span_id,
								 per_level_buffers[exec_nested_level].query_id);

		if (pg_tracing_planstate_spans)

			/*
			 * Setup ExecProcNode override to capture node start if planstate
			 * spans were requested
			 */
			setup_ExecProcNode_override(queryDesc, exec_nested_level);
	}

	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
	}
	PG_CATCH();
	{
		/*
		 * When a query is executing the pg_tracing_{peek,consume}_spans
		 * function, sampling will be disabled while we have an ongoing trace
		 * and current_trace_spans will be NULL.
		 */
		if (current_trace_spans != NULL && executor_sampled)
		{
			span_end_time = end_nested_level();
			handle_pg_error(trace_context, NULL, queryDesc, span_end_time);
		}
		else
			exec_nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * Same as above, tracing could have been aborted, check for
	 * current_trace_spans
	 */
	if (current_trace_spans != NULL && executor_sampled)
	{
		span_end_time = end_nested_level();
		/* End ExecutorRun span and store it */
		per_level_buffers[exec_nested_level].executor_end = span_end_time;
		end_latest_top_span(&span_end_time, true);
	}
	else
		exec_nested_level--;
}

/*
 * ExecutorFinish hook: create executor finish span and track nesting depth.
 * ExecutorFinish can start nested queries through triggers so we need
 * to set the ExecutorFinish span as the top span.
 */
static void
pg_tracing_ExecutorFinish(QueryDesc *queryDesc)
{
	pgTracingTraceContext *trace_context = &current_trace_context;
	uint64		executor_finish_span_id;

	TimestampTz span_end_time;
	bool		executor_sampled = pg_tracing_enabled(trace_context, exec_nested_level) && queryDesc->totaltime != NULL;

	if (executor_sampled)
	{
		TimestampTz span_start_time = GetCurrentTimestamp();
		uint64		parent_id = get_latest_top_span(exec_nested_level)->span_id;

		/* Create ExecutorFinish as a new potential top span */
		Span	   *executor_finish_span = allocate_new_top_span();

		begin_span(trace_context->traceparent.trace_id,
				   executor_finish_span, SPAN_EXECUTOR_FINISH,
				   NULL, parent_id,
				   per_level_buffers[exec_nested_level].query_id, &span_start_time);
		executor_finish_span_id = executor_finish_span->span_id;
	}

	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_CATCH();
	{
		if (current_trace_spans == NULL || !executor_sampled)
			exec_nested_level--;
		else
		{
			span_end_time = end_nested_level();
			handle_pg_error(trace_context, NULL, queryDesc, span_end_time);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (current_trace_spans == NULL || !executor_sampled)
	{
		exec_nested_level--;
		return;
	}
	span_end_time = end_nested_level();

	/*
	 * We only trace executorFinish when it has a nested query, check if we
	 * have a possible child
	 */
	if (max_nested_level > exec_nested_level)
	{
		Span	   *nested_span = get_latest_top_span(exec_nested_level + 1);

		/* Check if the child matches ExecutorFinish's id */
		if (nested_span->parent_id != executor_finish_span_id)
			/* No child for executor finish, discard it */
			pop_top_span();
		else
			/* End ExecutorFinish span and store it */
			end_latest_top_span(&span_end_time, true);
	}
	else
		/* No child for ExecutorFinish, discard it */
		pop_top_span();
}

/*
 * ExecutorEnd hook will:
 * - process queryDesc to extract informations from query instrumentation
 * - end top span for the current nested level
 * - end tracing if we're at the root level
 */
static void
pg_tracing_ExecutorEnd(QueryDesc *queryDesc)
{
	pgTracingTraceContext *trace_context = &current_trace_context;
	bool		executor_sampled = pg_tracing_enabled(trace_context, exec_nested_level) && queryDesc->totaltime != NULL;

	if (executor_sampled)
	{
		TimestampTz parent_end = per_level_buffers[exec_nested_level].executor_end;

		process_query_desc(trace_context, queryDesc, 0, parent_end);
		drop_traced_planstate(exec_nested_level);
	}

	/* No need to increment nested level here */
	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	if (executor_sampled)
	{
		TimestampTz span_end_time = GetCurrentTimestamp();
		bool		overlapping_trace_context;
		Span	   *top_span = get_latest_top_span(exec_nested_level);

		/* End top span */
		end_span(top_span, &span_end_time);

		/*
		 * if root trace context has a different root span index, it means
		 * that we may have started to trace the next statement while still
		 * processing the current one. This may happen with a transaction
		 * block using extended protocol, the parsing of the next statement
		 * happens before the ExecutorEnd of the current statement. In this
		 * case, we can't end tracing as we still have unfinished spans.
		 */
		overlapping_trace_context = exec_nested_level == 0 &&
			current_trace_context.root_span.span_id != root_trace_context.root_span.span_id &&
			root_trace_context.traceparent.sampled;
		if (overlapping_trace_context)
			store_span(top_span);
		else
			end_tracing(trace_context, NULL);
	}
}

/*
 * ProcessUtility hook
 *
 * Trace utility query if utility tracking is enabled and sampling was enabled
 * during parse step.
 * Process utility may create nested queries (for example function CALL) so we need
 * to set the ProcessUtility span as the top span before going through the standard
 * codepath.
 */
static void
pg_tracing_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
						  bool readOnlyTree,
						  ProcessUtilityContext context,
						  ParamListInfo params, QueryEnvironment *queryEnv,
						  DestReceiver *dest, QueryCompletion *qc)
{
	Span	   *process_utility_span;
	uint64		parent_id;
	TimestampTz span_end_time;
	TimestampTz span_start_time;

	/*
	 * Save track utility value since this value could be modified by a SET
	 * command
	 */
	bool		track_utility = pg_tracing_track_utility;
	pgTracingTraceContext *trace_context = &current_trace_context;

	if (exec_nested_level + plan_nested_level == 0)
	{
		/* We're at root level, copy the root trace_context */
		*trace_context = root_trace_context;
	}

	if (!track_utility || !pg_tracing_enabled(trace_context, exec_nested_level))
	{
		/*
		 * When utility tracking is disabled, we want to avoid tracing nested
		 * queries created by the utility statement. Incrementing the nested
		 * level will avoid starting a new nested trace while the root utility
		 * wasn't traced
		 */
		if (!track_utility)
			exec_nested_level++;

		/* No sampling, just go through the standard process utility */
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv,
								dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree,
									context, params, queryEnv,
									dest, qc);
		if (!track_utility)
			exec_nested_level--;

		/*
		 * Tracing may have been started within the utility call without going
		 * through ExecutorEnd (ex: prepare statement). Check and end tracing
		 * here in this case.
		 */
		if (!pg_tracing_mem_ctx->isReset && exec_nested_level == 0)
		{
			Assert(current_trace_context.traceparent.sampled ||
				   root_trace_context.traceparent.sampled);
			end_tracing(trace_context, NULL);
		}
		return;
	}

	/* Statement is sampled */
	span_start_time = GetCurrentTimestamp();

	parent_id = initialize_top_span(trace_context, pstmt->commandType, NULL,
									NULL, pstmt, queryString, span_start_time, false);
	process_utility_span = allocate_new_top_span();

	/* Build the process utility span. */
	begin_span(trace_context->traceparent.trace_id, process_utility_span,
			   SPAN_PROCESS_UTILITY, NULL, parent_id,
			   per_level_buffers[exec_nested_level].query_id, &span_start_time);

	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString, readOnlyTree, context,
								params, queryEnv, dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
									params, queryEnv, dest, qc);
	}
	PG_CATCH();
	{
		/*
		 * Tracing may have been reset within processUtility, check for
		 * current_trace_spans
		 */
		if (current_trace_spans != NULL)
		{
			span_end_time = end_nested_level();
			handle_pg_error(trace_context, NULL, NULL, span_end_time);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Same as above, abort if tracing was disabled within process utility */
	if (current_trace_spans == NULL)
	{
		exec_nested_level--;
		return;
	}
	span_end_time = end_nested_level();

	/* buffer may have been repalloced, grab a fresh pointer */
	process_utility_span = get_latest_top_span(exec_nested_level);
	if (qc != NULL)
		process_utility_span->node_counters.rows = qc->nprocessed;

	/* End ProcessUtility span and store it */
	end_latest_top_span(&span_end_time, true);

	/*
	 * We're at a potential end for the query, end the parent top span that
	 * was restored and end tracing
	 */
	end_latest_top_span(&span_end_time, false);
	end_tracing(trace_context, NULL);
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
add_node_counters(const NodeCounters * node_counters, int i, Datum *values)
{
	Datum		wal_bytes;
	char		buf[256];
	double		blk_read_time,
				blk_write_time,
				temp_blk_read_time,
				temp_blk_write_time;
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

	temp_blk_read_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.temp_blk_read_time);
	temp_blk_write_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.temp_blk_write_time);

	values[i++] = Float8GetDatumFast(blk_read_time);
	values[i++] = Float8GetDatumFast(blk_write_time);
	values[i++] = Float8GetDatumFast(temp_blk_read_time);
	values[i++] = Float8GetDatumFast(temp_blk_write_time);

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
 * Build the tuple for a Span and add it to the output
 */
static void
add_result_span(ReturnSetInfo *rsinfo, Span * span,
				const char *qbuffer, Size qbuffer_size)
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

	span_type = get_span_type(span, qbuffer, qbuffer_size);
	operation_name = get_operation_name(span, qbuffer, qbuffer_size);
	sql_error_code = unpack_sql_state(span->sql_error_code);

	pg_snprintf(trace_id, 33, INT64_HEX_FORMAT INT64_HEX_FORMAT,
				span->trace_id.traceid_left,
				span->trace_id.traceid_right);
	pg_snprintf(parent_id, 17, INT64_HEX_FORMAT, span->parent_id);
	pg_snprintf(span_id, 17, INT64_HEX_FORMAT, span->span_id);

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
		i = add_node_counters(&span->node_counters, i, values);
		values[i++] = Int64GetDatumFast(span->startup);

		if (span->parameter_offset != -1 && qbuffer_size > 0 && qbuffer_size > span->parameter_offset)
			values[i++] = CStringGetTextDatum(qbuffer + span->parameter_offset);
		else
			nulls[i++] = 1;

		if (span->deparse_info_offset != -1 && qbuffer_size > 0 && qbuffer_size > span->deparse_info_offset)
			values[i++] = CStringGetTextDatum(qbuffer + span->deparse_info_offset);
	}

	for (int j = i; j < PG_TRACING_TRACES_COLS; j++)
		nulls[j] = 1;

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

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
	const char *qbuffer;
	Size		qbuffer_size = 0;
	LWLockMode	lock_mode = LW_SHARED;

	consume = PG_GETARG_BOOL(0);

	/*
	 * We need an exclusive lock to truncate and empty the shared buffer when
	 * we consume
	 */
	if (consume)
		lock_mode = LW_EXCLUSIVE;

	if (!pg_tracing)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_tracing must be loaded via shared_preload_libraries")));
	InitMaterializedSRF(fcinfo, 0);

	/*
	 * If this query was sampled and we're consuming tracing_spans buffer, the
	 * spans will target a query string that doesn't exist anymore in the
	 * query file. Better abort the sampling and clean ongoing traces. Since
	 * this will be called within an ExecutorRun, we will need to check for
	 * current_trace_spans at the end of the ExecutorRun hook.
	 */
	cleanup_tracing();

	qbuffer = qtext_load_file(&qbuffer_size);
	if (qbuffer == NULL)

		/*
		 * It's possible to get NULL if file was truncated while we read it.
		 * Abort in this case.
		 */
		return (Datum) 0;

	LWLockAcquire(pg_tracing->lock, lock_mode);
	for (int i = 0; i < shared_spans->end; i++)
	{
		span = shared_spans->spans + i;
		add_result_span(rsinfo, span, qbuffer, qbuffer_size);
	}

	/* Consume is set, remove spans from the shared buffer */
	if (consume)
		drop_all_spans_locked();
	pg_tracing->stats.last_consume = GetCurrentTimestamp();
	LWLockRelease(pg_tracing->lock);

	return (Datum) 0;
}

/*
 * Return statistics of pg_tracing.
 */
Datum
pg_tracing_info(PG_FUNCTION_ARGS)
{
#define PG_TRACING_INFO_COLS	6
	pgTracingStats stats;
	TupleDesc	tupdesc;
	Datum		values[PG_TRACING_INFO_COLS] = {0};
	bool		nulls[PG_TRACING_INFO_COLS] = {0};
	int			i = 0;

	if (!pg_tracing)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_tracing must be loaded via shared_preload_libraries")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Get a copy of the pg_tracing stats */
	LWLockAcquire(pg_tracing->lock, LW_SHARED);
	stats = pg_tracing->stats;
	LWLockRelease(pg_tracing->lock);

	values[i++] = Int64GetDatum(stats.processed_traces);
	values[i++] = Int64GetDatum(stats.processed_spans);
	values[i++] = Int64GetDatum(stats.dropped_traces);
	values[i++] = Int64GetDatum(stats.dropped_spans);
	values[i++] = TimestampTzGetDatum(stats.last_consume);
	values[i++] = TimestampTzGetDatum(stats.stats_reset);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * Get an empty pgTracingStats
 */
static pgTracingStats
get_empty_pg_tracing_stats(void)
{
	pgTracingStats stats;

	stats.processed_traces = 0;
	stats.processed_spans = 0;
	stats.dropped_traces = 0;
	stats.dropped_spans = 0;
	stats.last_consume = 0;
	stats.stats_reset = GetCurrentTimestamp();
	return stats;
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

	LWLockAcquire(pg_tracing->lock, LW_EXCLUSIVE);
	pg_tracing->stats = empty_stats;
	LWLockRelease(pg_tracing->lock);

	PG_RETURN_VOID();
}
