/*-------------------------------------------------------------------------
 *
 * pg_tracing.c
 *		Generate spans for distributed tracing from SQL query
 *
 * Spans will only be generated for sampled queries. A query is sampled if:
 * - It has a tracecontext propagated through SQLCommenter and it passes the
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
 * represents an operation with a start time, an end time and metadata (block
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
 *	  src/pg_tracing.c
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

/*
 * Global variables
 */

/* Memory context for pg_tracing. */
MemoryContext pg_tracing_mem_ctx;

/* trace context at the root level of parse/planning hook */
static struct pgTracingTraceContext parsed_trace_context;

/* trace context used in nested levels or within executor hooks */
static struct pgTracingTraceContext executor_trace_context;

/* traceparent extracted at the start of a transaction */
static pgTracingTraceparent tx_start_traceparent;

/* Latest local transaction id traced */
static LocalTransactionId latest_lxid = InvalidLocalTransactionId;

/* Shared state with stats and file external state */
pgTracingSharedState *pg_tracing_shared_state = NULL;

/*
 * Shared buffer storing spans. Query with sampled flag will add new spans to the
 * shared state at the end of the traced query.
 * Those spans will be consumed during calls to pg_tracing_consume_spans.
 */
pgTracingSpans *shared_spans = NULL;

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

/* Current nesting depth of ExecutorRun+ProcessUtility calls */
int			exec_nested_level = 0;

/*
 * Maximum nested level for a query to know how many top spans we need to
 * copy in shared_spans.
 */
int			max_nested_level = -1;

/* Whether we're in a cursor declaration */
static bool within_declare_cursor = false;

/* Number of spans initially allocated at the start of a trace. */
static int	pg_tracing_initial_allocated_spans = 25;

/* Commit span used in xact callbacks */
static Span commit_span;

static pgTracingQueryIdFilter * query_id_filter = NULL;
pgTracingPerLevelBuffer *per_level_buffers = NULL;

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

static bool check_filter_query_ids(char **newval, void **extra, GucSource source);
static void assign_filter_query_ids(const char *newval, void *extra);

static void initialize_trace_level(void);

static void pg_tracing_xact_callback(XactEvent event, void *arg);

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

	RegisterXactCallback(pg_tracing_xact_callback, NULL);
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
 * End all spans for the current nested level
 */
static TimestampTz
end_nested_level(void)
{
	TimestampTz span_end_time = GetCurrentTimestamp();
	Span	   *top_span;

	if (exec_nested_level > max_nested_level)
		/* No nested level were created */
		return span_end_time;

	top_span = peek_top_span();
	if (top_span == NULL && parsed_trace_context.root_span.span_id > 0 && exec_nested_level == 0)

		/*
		 * If we only had a parse command (like calling \gdesc), the span will
		 * only be stored in the parsed_trace_context, use it
		 */
		top_span = &parsed_trace_context.root_span;

	while (top_span != NULL && top_span->nested_level == exec_nested_level)
	{
		TimestampTz top_span_end = span_end_time;

		if (!top_span->ended)
		{
			if (top_span->parent_planstate_index > -1)
			{
				/* We have a parent planstate, use it for the span end */
				TracedPlanstate *traced_planstate = get_traced_planstate_from_index(top_span->parent_planstate_index);

				/* Make sure to end instrumentation */
				InstrEndLoop(traced_planstate->planstate->instrument);
				top_span_end = get_span_end_from_planstate(traced_planstate->planstate, traced_planstate->node_start, span_end_time);
			}
			end_span(top_span, &top_span_end);
			store_span(top_span);
		}
		pop_top_span();
		top_span = peek_top_span();
	}
	return span_end_time;
}

/*
 * Drop all spans from the shared buffer and truncate the query file.
 * Exclusive lock on pg_tracing->lock should be acquired beforehand.
 */
void
drop_all_spans_locked(void)
{
	/* Drop all spans */
	shared_spans->end = 0;

	/* Reset query file position */
	pg_tracing_shared_state->extent = 0;
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

	LWLockAcquire(pg_tracing_shared_state->lock, LW_SHARED);
	full_buffer = shared_spans->end >= shared_spans->max;
	if (full_buffer && pg_tracing_buffer_mode != PG_TRACING_DROP_ON_FULL)
	{
		/*
		 * Buffer is full and we want to keep existing spans. Drop the new
		 * trace.
		 */
		pg_tracing_shared_state->stats.dropped_traces++;
		LWLockRelease(pg_tracing_shared_state->lock);
		return full_buffer;
	}
	LWLockRelease(pg_tracing_shared_state->lock);

	if (!full_buffer)
		/* We have room in the shared buffer */
		return full_buffer;

	/*
	 * We have a full buffer and we want to drop everything, get an exclusive
	 * lock
	 */
	LWLockAcquire(pg_tracing_shared_state->lock, LW_EXCLUSIVE);
	/* Recheck for full buffer */
	full_buffer = shared_spans->end >= shared_spans->max;
	if (full_buffer)
	{
		pg_tracing_shared_state->stats.dropped_spans += shared_spans->end;
		drop_all_spans_locked();
	}
	LWLockRelease(pg_tracing_shared_state->lock);

	return full_buffer;
}

/*
 * Reset traceparent fields
 */
static void
reset_traceparent(pgTracingTraceparent * traceparent)
{
	traceparent->sampled = 0;
	traceparent->trace_id.traceid_right = 0;
	traceparent->trace_id.traceid_left = 0;
	traceparent->parent_id = 0;
}

/*
 * Reset trace_context fields
 */
static void
reset_trace_context(pgTracingTraceContext * trace_context)
{
	reset_traceparent(&trace_context->traceparent);
	trace_context->root_span.span_id = 0;
}

static void
update_latest_lxid()
{
#if PG_VERSION_NUM >= 170000
	latest_lxid = MyProc->vxid.lxid;
#else
	latest_lxid = MyProc->lxid;
#endif
}

static bool
is_new_lxid()
{
#if PG_VERSION_NUM >= 170000
	return MyProc->vxid.lxid != latest_lxid;
#else
	return MyProc->lxid != latest_lxid;
#endif
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
	pg_tracing_shared_state = NULL;

	/* Create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	reset_trace_context(&parsed_trace_context);
	reset_trace_context(&executor_trace_context);
	pg_tracing_shared_state = ShmemInitStruct("PgTracing Shared", sizeof(pgTracingSharedState),
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
		pg_tracing_shared_state->stats = get_empty_pg_tracing_stats();
		pg_tracing_shared_state->lock = &(GetNamedLWLockTranche("pg_tracing"))->lock;
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
	NodeCounters *node_counters = &peek_top_span()->node_counters;

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
#if (PG_VERSION_NUM >= 160000)
	node_counters->rows = queryDesc->estate->es_total_processed;
#else
	node_counters->rows = queryDesc->estate->es_processed;
#endif


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
	bool		new_lxid = is_new_lxid();

	/* trace id was already set, use it */
	if (!traceid_zero(trace_context->traceparent.trace_id))
	{
		/*
		 * Keep track of the trace context in case there are multiple
		 * statement in the transaction
		 */
		if (new_lxid)
			tx_start_traceparent = trace_context->traceparent;
		return;
	}

	/*
	 * We want to keep the same trace id for all statements within the same
	 * transaction. For that, we check if we're in the same local xid.
	 */
	if (!new_lxid)
	{
		/* We're in the same transaction, use the transaction trace context */
		Assert(!traceid_zero(tx_start_traceparent.trace_id));
		trace_context->traceparent = tx_start_traceparent;
		return;
	}

	/*
	 * We leave parent_id to 0 as a way to indicate that this is a standalone
	 * trace.
	 */
	Assert(trace_context->traceparent.parent_id == 0);

	trace_context->traceparent.trace_id.traceid_left = pg_prng_int64(&pg_global_prng_state);
	trace_context->traceparent.trace_id.traceid_right = pg_prng_int64(&pg_global_prng_state);
	/* We're at the begining of a new local transaction, save trace context */
	tx_start_traceparent = trace_context->traceparent;
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
	if (!pg_tracing_shared_state || !pg_tracking_level(exec_nested_level))
		goto cleanup;

	/* sampling already started */
	if (trace_context->traceparent.sampled)
		goto cleanup;

	/* Don't start tracing if we're not at the root level */
	if (exec_nested_level > 0)
		goto cleanup;

	/* Both sampling rate are set to 0, no tracing will happen */
	if (pg_tracing_sample_rate == 0 && pg_tracing_caller_sample_rate == 0)
		goto cleanup;

	/*
	 * In a parallel worker, check the parallel context shared buffer to see
	 * if the leader left a trace context
	 */
	if (IsParallelWorker())
	{
		if (pg_tracing_trace_parallel_workers)
			fetch_parallel_context(trace_context);
		goto cleanup;
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
		goto cleanup;
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

cleanup:
	/* No matter what happens, we want to update the latest_lxid seen */
	update_latest_lxid();
}

/*
 * Reset pg_tracing memory context and global state.
 */
void
cleanup_tracing(void)
{
	if (!parsed_trace_context.traceparent.sampled &&
		!executor_trace_context.traceparent.sampled)
		/* No need for cleaning */
		return;
	cleanup_top_spans();
	MemoryContextReset(pg_tracing_mem_ctx);
	reset_trace_context(&parsed_trace_context);
	reset_trace_context(&executor_trace_context);
	max_nested_level = -1;
	within_declare_cursor = false;
	current_trace_spans = NULL;
	per_level_buffers = NULL;
	cleanup_planstarts();
}

/*
 * Add span to the shared memory.
 * Exclusive lock on pg_tracing_shared_state->lock must be acquired beforehand.
 */
static void
add_span_to_shared_buffer_locked(const Span * span)
{
	/* Spans must be ended before adding them to the shared buffer */
	Assert(span->ended);
	if (shared_spans->end >= shared_spans->max)
		pg_tracing_shared_state->stats.dropped_spans++;
	else
	{
		pg_tracing_shared_state->stats.processed_spans++;
		shared_spans->spans[shared_spans->end++] = *span;
	}
}

/*
 * End the query tracing and dump all spans in the shared buffer if we are at
 * the root level. This may happen either when query is finished or on a caught error.
 */
static void
end_tracing(pgTracingTraceContext * trace_context)
{
	Size		file_position = 0;

	/* We're still a nested query, tracing is not finished */
	if (exec_nested_level > 0)
		return;

	LWLockAcquire(pg_tracing_shared_state->lock, LW_EXCLUSIVE);

	if (current_trace_text->len > 0)
	{
		/* Dump all buffered texts in file */
		text_store_file(pg_tracing_shared_state, current_trace_text->data, current_trace_text->len,
						&file_position);

		/* Adjust file position of spans */
		for (int i = 0; i < current_trace_spans->end; i++)
			adjust_file_offset(current_trace_spans->spans + i, file_position);
	}

	/* We're at the end, add all stored spans to the shared memory */
	for (int i = 0; i < current_trace_spans->end; i++)
		add_span_to_shared_buffer_locked(&current_trace_spans->spans[i]);

	/* Update our stats with the new trace */
	pg_tracing_shared_state->stats.processed_traces++;
	LWLockRelease(pg_tracing_shared_state->lock);

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
	Span	   *top_span;

	/* If we're not sampling the query, bail out */
	if (!pg_tracing_enabled(trace_context, exec_nested_level))
		return;

	sql_error_code = geterrcode();

	if (queryDesc != NULL)
		process_query_desc(trace_context, queryDesc, sql_error_code, span_end_time);

	top_span = pop_top_span();
	while (top_span != NULL)
	{
		if (!top_span->ended)
		{
			/* Assign the error code to the latest top span */
			top_span->sql_error_code = sql_error_code;
			end_span(top_span, &span_end_time);
			store_span(top_span);
		}
		top_span = pop_top_span();
	}

	if (ongoing_span != NULL)
	{
		ongoing_span->sql_error_code = sql_error_code;
		if (!ongoing_span->ended)
			end_span(ongoing_span, &span_end_time);
		store_span(ongoing_span);
	}
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
		MemoryContextSwitchTo(oldcxt);
	}
	max_nested_level = exec_nested_level;
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
	pgTracingTraceContext *trace_context = &parsed_trace_context;
	bool		new_lxid = is_new_lxid();
	bool		is_root_level = exec_nested_level == 0;

	if (new_lxid)
		/* We have a new local transaction, reset the begin tx traceparent */
		reset_traceparent(&tx_start_traceparent);

	if (!pg_tracing_mem_ctx->isReset && new_lxid && is_root_level)

		/*
		 * Some errors can happen outside of our PG_TRY (incorrect number of
		 * bind parameters for example) which will leave a dirty trace
		 * context. We can also have sampled individual Parse command through
		 * extended protocol. In this case, we just drop the spans and reset
		 * memory context
		 */
		cleanup_tracing();

	if (!is_root_level)
		/* We're in a nested query, grab the ongoing trace_context */
		trace_context = &executor_trace_context;
	else
		/* At root level, we will start a new root span */
		reset_trace_context(&parsed_trace_context);

	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query, jstate);

	/* If disabled, don't trace utility statement */
	if (query->utilityStmt && !pg_tracing_track_utility)
	{
		update_latest_lxid();
		return;
	}

	/* Evaluate if query is sampled or not */
	extract_trace_context(trace_context, pstate, query->queryId);

	if (!trace_context->traceparent.sampled)
	{
		if (is_root_level && !new_lxid && tx_start_traceparent.sampled)
		{
			/*
			 * We're in the same local transaction, use the trace context
			 * extracted at the begining of the transaction
			 */
			Assert(!traceid_zero(tx_start_traceparent.trace_id));
			trace_context->traceparent = tx_start_traceparent;
		}
		else
		{
			/* Query is not sampled, nothing to do. */
			return;
		}
	}

	/*
	 * We want to avoid calling GetCurrentTimestamp at the start of post parse
	 * as it will impact all queries and we will only use it when the query is
	 * sampled.
	 */
	start_top_span = GetCurrentTimestamp();

	initialize_trace_level();

	/*
	 * Either we're inside a nested sampled query or we've parsed a query with
	 * the sampled flag, start a new level with a top span
	 */
	initialize_top_span(trace_context, query->commandType,
						query, jstate, NULL,
						pstate->p_sourcetext, start_top_span,
						true, pg_tracing_export_parameters);
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
	pgTracingTraceContext *trace_context = &parsed_trace_context;
	TimestampTz span_start_time;
	TimestampTz span_end_time;

	if (exec_nested_level > 0)
	{
		if (!executor_trace_context.traceparent.sampled
			&& parsed_trace_context.traceparent.sampled)

			/*
			 * If we have nested planning, we need to use the
			 * parsed_trace_context
			 */
			trace_context = &parsed_trace_context;
		else
			/* We're in a nested query, grab the ongoing trace_context */
			trace_context = &executor_trace_context;
	}

	/* Evaluate if query is sampled or not */
	extract_trace_context(trace_context, NULL, query->queryId);

	if (!pg_tracing_enabled(trace_context, exec_nested_level))
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

	/* statement is sampled */

	span_start_time = GetCurrentTimestamp();

	initialize_trace_level();

	parent_id = initialize_top_span(trace_context, query->commandType, query,
									NULL, NULL, query_string, span_start_time, true, pg_tracing_export_parameters);

	/* Create and start the planner span */
	span_planner = allocate_new_top_span();
	begin_span(trace_context->traceparent.trace_id, span_planner, SPAN_PLANNER,
			   NULL, parent_id,
			   per_level_buffers[exec_nested_level].query_id, &span_start_time);

	exec_nested_level++;
	PG_TRY();
	{
		if (prev_planner_hook)
			result = prev_planner_hook(query, query_string, cursorOptions, params);
		else
			result = standard_planner(query, query_string, cursorOptions, params);
	}
	PG_CATCH();
	{
		Span	   *root_span = NULL;

		span_end_time = GetCurrentTimestamp();

		exec_nested_level--;
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
	span_end_time = end_nested_level();
	exec_nested_level--;

	/* End planner span */
	end_latest_top_span(&span_end_time);

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
				top_span = peek_top_span();
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
	pgTracingTraceContext *trace_context = &executor_trace_context;
	bool		executor_sampled;
	bool		is_lazy_function;

	if (exec_nested_level == 0)
	{
		/* We're at the root level, copy trace context from parsing/planning */
		*trace_context = parsed_trace_context;
		reset_trace_context(&parsed_trace_context);
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

		initialize_trace_level();

		/*
		 * In case of a cached plan, we haven't gone through neither parsing
		 * nor planner hook. Create the top case in this case.
		 */
		initialize_top_span(trace_context, queryDesc->operation, NULL, NULL, NULL,
							queryDesc->sourceText, start_span_time, false, pg_tracing_export_parameters);

		/*
		 * We only need full instrumentation if we generate spans from
		 * planstate.
		 *
		 * If we're within a cursor declaration, we won't be able to generate
		 * spans from planstate due to cursor behaviour. Fetches will only
		 * call ExecutorRun and Closing the cursor will call ExecutorFinish
		 * and ExecutorEnd. We won't have the start of the planstate available
		 * and representing a node that has a fragmented execution doesn't fit
		 * our current model. So we disable full query instrumentation for
		 * cursors.
		 */
		if (pg_tracing_planstate_spans && !within_declare_cursor)
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
	pgTracingTraceContext *trace_context = &executor_trace_context;
	TimestampTz span_end_time;
	bool		executor_sampled = pg_tracing_enabled(trace_context, exec_nested_level) && queryDesc->totaltime != NULL;

	if (executor_sampled)
	{
		Span	   *executor_run_span;
		TimestampTz span_start_time = GetCurrentTimestamp();
		uint64		parent_id;

		initialize_trace_level();

		/*
		 * When fetching an existing cursor, the portal already exists and
		 * ExecutorRun is the first hook called. Create the top span if it
		 * doesn't already exist.
		 */
		parent_id = initialize_top_span(trace_context, queryDesc->operation, NULL, NULL, NULL,
										queryDesc->sourceText, span_start_time, false, pg_tracing_export_parameters);

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

		/*
		 * Setup ExecProcNode override to capture node start if planstate
		 * spans were requested. If there's no query instrumentation, we can
		 * skip ExecProcNode override.
		 */
		if (pg_tracing_planstate_spans && queryDesc->planstate->instrument)
			setup_ExecProcNode_override(queryDesc);
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
			exec_nested_level--;
			handle_pg_error(trace_context, NULL, queryDesc, span_end_time);
		}
		else
			exec_nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* We can remove our parallel context here */
	if (queryDesc->plannedstmt->parallelModeNeeded && pg_tracing_trace_parallel_workers)
		remove_parallel_context();

	/*
	 * Same as above, tracing could have been aborted, check for
	 * current_trace_spans
	 */
	if (current_trace_spans != NULL && executor_sampled)
	{
		span_end_time = end_nested_level();
		exec_nested_level--;
		/* End ExecutorRun span and store it */
		per_level_buffers[exec_nested_level].executor_end = span_end_time;
		end_latest_top_span(&span_end_time);
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
	pgTracingTraceContext *trace_context = &executor_trace_context;

	TimestampTz span_end_time;
	bool		executor_sampled = pg_tracing_enabled(trace_context, exec_nested_level) && queryDesc->totaltime != NULL;
	int			num_stored_spans = 0;

	if (current_trace_spans != NULL)
		num_stored_spans = current_trace_spans->end;

	if (executor_sampled)
	{
		TimestampTz span_start_time = GetCurrentTimestamp();
		uint64		parent_id;
		Span	   *executor_finish_span;

		initialize_trace_level();

		/*
		 * When closing a cursor, only ExecutorFinish and ExecutorEnd will be
		 * called. Create the top span in this case.
		 */
		parent_id = initialize_top_span(trace_context, queryDesc->operation, NULL, NULL, NULL,
										queryDesc->sourceText, span_start_time, false, pg_tracing_export_parameters);

		/* Create ExecutorFinish as a new potential top span */
		executor_finish_span = allocate_new_top_span();

		begin_span(trace_context->traceparent.trace_id,
				   executor_finish_span, SPAN_EXECUTOR_FINISH,
				   NULL, parent_id,
				   per_level_buffers[exec_nested_level].query_id, &span_start_time);
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
			exec_nested_level--;
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
	exec_nested_level--;

	/*
	 * We only trace executorFinish when it has a nested query, check if we
	 * have a possible child
	 */
	if (current_trace_spans->end > num_stored_spans)
		end_latest_top_span(&span_end_time);
	else
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
	pgTracingTraceContext *trace_context = &executor_trace_context;
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
		Span	   *top_span = pop_top_span();

		/* End top span */
		end_span(top_span, &span_end_time);
		store_span(top_span);

		/*
		 * Special case: we're inside a transaction block and a single
		 * statement was traced, not the whole transaction. We can't rely on
		 * the xact callback to end tracing so do it here
		 */
		if (IsInTransactionBlock(true) && tx_start_traceparent.sampled == false && exec_nested_level == 0)
		{
			end_tracing(trace_context);
		}
	}
}

/*
 * ProcessUtility hook
 *
 * Trace utility query if utility tracking is enabled and sampling was enabled
 * during parse step.
 * Process utility may create nested queries (for example function CALL) so we need
 * to set the ProcessUtility span as the top span before going through the standard
 * code path.
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
	Node	   *parsetree;

	/*
	 * Save whether we're in an aborted transaction. A rollback will reset the
	 * state after standard_ProcessUtility
	 */
	bool		in_aborted_transaction = IsAbortedTransactionBlockState();

	/*
	 * Save track utility value since this value could be modified by a SET
	 * command
	 */
	bool		track_utility = pg_tracing_track_utility;
	pgTracingTraceContext *trace_context = &executor_trace_context;

	if (exec_nested_level == 0)
	{
		/* We're at root level, copy the root trace_context */
		*trace_context = parsed_trace_context;
		reset_trace_context(&parsed_trace_context);
	}

	parsetree = pstmt->utilityStmt;

	/*
	 * Keep track if we're in a declare cursor as we want to disable query
	 * instrumentation in this case.
	 */
	if (nodeTag(parsetree) == T_DeclareCursorStmt)
		within_declare_cursor = true;
	else
		within_declare_cursor = false;

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

		return;
	}

	/* Statement is sampled */
	span_start_time = GetCurrentTimestamp();

	initialize_trace_level();

	parent_id = initialize_top_span(trace_context, pstmt->commandType, NULL,
									NULL, pstmt, queryString, span_start_time, false, pg_tracing_export_parameters);
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
		 * Tracing may have been reset within ProcessUtility, check for
		 * current_trace_spans
		 */
		if (current_trace_spans != NULL)
		{
			span_end_time = end_nested_level();
			exec_nested_level--;
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
	exec_nested_level--;

	/* buffer may have been repalloced, grab a fresh pointer */
	process_utility_span = peek_top_span();
	if (qc != NULL)
		process_utility_span->node_counters.rows = qc->nprocessed;

	/* End ProcessUtility span and store it */
	end_latest_top_span(&span_end_time);
	end_latest_top_span(&span_end_time);

	/*
	 * If we're in an aborted transaction, xact callback won't be called so we
	 * need to end tracing here
	 */
	if (in_aborted_transaction)
		end_tracing(trace_context);
}

/*
 * Handle xact callback events
 *
 * Create a commit span between pre-commit and commit event and end ongoing tracing
 */
static void
pg_tracing_xact_callback(XactEvent event, void *arg)
{
	pgTracingTraceContext *trace_context = &executor_trace_context;

	if (!parsed_trace_context.traceparent.sampled &&
		!executor_trace_context.traceparent.sampled)
		return;

	if (!executor_trace_context.traceparent.sampled)
		trace_context = &parsed_trace_context;

	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
			begin_span(trace_context->traceparent.trace_id, &commit_span,
					   SPAN_COMMIT, NULL, trace_context->traceparent.parent_id,
					   per_level_buffers[exec_nested_level].query_id, NULL);
			break;
		case XACT_EVENT_COMMIT:
			Assert(commit_span.span_id > 0);
			end_nested_level();
			end_span(&commit_span, NULL);
			store_span(&commit_span);

			end_tracing(trace_context);
			commit_span.span_id = 0;
			break;
		case XACT_EVENT_ABORT:
			end_tracing(trace_context);
			commit_span.span_id = 0;
		default:
			break;
	}
}
