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

#include "access/parallel.h"
#include "executor/executor.h"
#include "access/xact.h"
#include "common/pg_prng.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "pg_tracing.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/utility.h"
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

/*
 * Structure to store per exec level informations
 */
typedef struct pgTracingPerLevelInfos
{
	uint64		executor_run_span_id;	/* executor run span id for this
										 * level. Executor run is used as
										 * parent for spans generated from
										 * planstate */
	TimestampTz executor_start;
	TimestampTz executor_end;
}			pgTracingPerLevelInfos;

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

#define pg_tracing_enabled(traceparent, level) \
	(traceparent->sampled && pg_tracking_level(level))

/*
 * Global variables
 */

/*
 * Memory context for pg_tracing.
 *
 * We can't rely on memory context like TopTransactionContext since
 * some queries like vacuum analyze will end and start their own
 * transactions while we still have active span
 */
MemoryContext pg_tracing_mem_ctx;

/* trace context at the root level of parse/planning hook */
static pgTracingTraceparent parse_traceparent;

/* trace context used in nested levels or within executor hooks */
static pgTracingTraceparent executor_traceparent;

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

/* Current nesting depth of Planner+ExecutorRun+ProcessUtility calls */
int			nested_level = 0;

/* Whether we're in a cursor declaration */
static bool within_declare_cursor = false;

/* Commit span used in xact callbacks */
static Span commit_span;

/*
 * query id at level 0 of the current tracing.
 * Used to assign queryId to the commit span
 */
static uint64 current_query_id;

static pgTracingQueryIdFilter * query_id_filter = NULL;
static pgTracingPerLevelInfos * per_level_infos = NULL;

/* Number of spans initially allocated at the start of a trace. */
#define	INITIAL_ALLOCATED_SPANS 25

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
static PlannedStmt *pg_tracing_planner_hook(Query *query,
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
 * Convert a node CmdType to the matching SpanType
 */
static SpanType
command_type_to_span_type(CmdType cmd_type)
{
	switch (cmd_type)
	{
		case CMD_SELECT:
			return SPAN_TOP_SELECT;
		case CMD_INSERT:
			return SPAN_TOP_INSERT;
		case CMD_UPDATE:
			return SPAN_TOP_UPDATE;
		case CMD_DELETE:
			return SPAN_TOP_DELETE;
		case CMD_MERGE:
			return SPAN_TOP_MERGE;
		case CMD_UTILITY:
			return SPAN_TOP_UTILITY;
		case CMD_NOTHING:
			return SPAN_TOP_NOTHING;
		case CMD_UNKNOWN:
			return SPAN_TOP_UNKNOWN;
	}
	return SPAN_TOP_UNKNOWN;
}

/*
 * Store a span in the current_trace_spans buffer
 */
void
store_span(const Span * span)
{
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
static void
end_nested_level(const TimestampTz *input_span_end_time)
{
	Span	   *span;
	TimestampTz span_end_time;

	span = peek_active_span();
	if (span == NULL || span->nested_level < nested_level)
		return;

	if (input_span_end_time != NULL)
		span_end_time = *input_span_end_time;
	else
		span_end_time = GetCurrentTimestamp();

	while (span != NULL && span->nested_level == nested_level)
	{
		if (span->parent_planstate_index > -1)
		{
			/* We have a parent planstate, use it for the span end */
			TracedPlanstate *traced_planstate = get_traced_planstate_from_index(span->parent_planstate_index);

			/* Make sure to end instrumentation */
			InstrEndLoop(traced_planstate->planstate->instrument);
			span_end_time = get_span_end_from_planstate(traced_planstate->planstate, traced_planstate->node_start, span_end_time);
		}
		pop_active_span(&span_end_time);
		span = peek_active_span();
	}
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

	reset_traceparent(&parse_traceparent);
	reset_traceparent(&executor_traceparent);
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
process_query_desc(const pgTracingTraceparent * traceparent, const QueryDesc *queryDesc,
				   int sql_error_code, TimestampTz parent_end)
{
	NodeCounters *node_counters = &peek_active_span()->node_counters;

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
		uint64		parent_id = per_level_infos[nested_level].executor_run_span_id;
		uint64		query_id = queryDesc->plannedstmt->queryId;
		TimestampTz parent_start = per_level_infos[nested_level].executor_start;

		planstateTraceContext.rtable_names = select_rtable_names_for_explain(queryDesc->plannedstmt->rtable, rels_used);
		planstateTraceContext.trace_id = traceparent->trace_id;
		planstateTraceContext.ancestors = NULL;
		planstateTraceContext.sql_error_code = sql_error_code;
		/* Prepare the planstate context for deparsing */
		planstateTraceContext.deparse_ctx = NULL;
		if (pg_tracing_deparse_plan)
			planstateTraceContext.deparse_ctx =
				deparse_context_for_plan_tree(queryDesc->plannedstmt,
											  planstateTraceContext.rtable_names);

		generate_span_from_planstate(queryDesc->planstate, &planstateTraceContext,
									 parent_id, query_id, parent_start,
									 parent_end, &latest_end);
	}
}

/*
 * Create a trace id for the trace context if there's none.
 * If trace was started from the global sample rate without a parent trace, we
 * need to generate a random trace id.
 */
static void
set_trace_id(pgTracingTraceparent * traceparent)
{
	bool		new_lxid = is_new_lxid();

	/* trace id was already set, use it */
	if (!traceid_zero(traceparent->trace_id))
	{
		/*
		 * Keep track of the trace context in case there are multiple
		 * statement in the transaction
		 */
		if (new_lxid)
			tx_start_traceparent = *traceparent;
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
		*traceparent = tx_start_traceparent;
		return;
	}

	/*
	 * We leave parent_id to 0 as a way to indicate that this is a standalone
	 * trace.
	 */
	Assert(traceparent->parent_id == 0);

	traceparent->trace_id.traceid_left = pg_prng_int64(&pg_global_prng_state);
	traceparent->trace_id.traceid_right = pg_prng_int64(&pg_global_prng_state);
	/* We're at the begining of a new local transaction, save trace context */
	tx_start_traceparent = *traceparent;
}

/*
 * Check whether trace context is filtered based on query id
 */
static bool
is_query_id_filtered(uint64 query_id)
{
	/* No query id filter defined */
	if (query_id_filter == NULL)
		return false;
	for (int i = 0; i < query_id_filter->num_query_id; i++)
		if (query_id_filter->query_ids[i] == query_id)
			return false;
	return true;
}

/*
 * Decide whether a query should be sampled depending on the traceparent sampled
 * flag and the provided sample rate configurations
 */
static bool
is_query_sampled(const pgTracingTraceparent * traceparent)
{
	double		rand;
	bool		sampled;

	/* Everything is sampled */
	if (pg_tracing_sample_rate >= 1.0)
		return true;

	/* No SQLCommenter sampled and no global sample rate */
	if (!traceparent->sampled && pg_tracing_sample_rate == 0.0)
		return false;

	/* SQLCommenter sampled and caller sample is on */
	if (traceparent->sampled && pg_tracing_caller_sample_rate >= 1.0)
		return true;

	/*
	 * We have a non zero global sample rate or a sampled flag. Either way, we
	 * need a rand value
	 */
	rand = pg_prng_double(&pg_global_prng_state);
	if (traceparent->sampled)
		/* Sampled flag case */
		sampled = (rand < pg_tracing_caller_sample_rate);
	else
		/* Global rate case */
		sampled = (rand < pg_tracing_sample_rate);
	return sampled;
}

/*
 * Extract traceparent from the query text.
 * Sampling rate will be applied to the traceparent.
 */
static void
extract_trace_context(pgTracingTraceparent * traceparent, ParseState *pstate,
					  uint64 query_id)
{
	/* Timestamp of the latest statement checked for sampling. */
	static TimestampTz last_statement_check_for_sampling = 0;
	TimestampTz statement_start_ts;

	/* Safety check... */
	if (!pg_tracing_shared_state || !pg_tracking_level(nested_level))
		goto cleanup;

	/* sampling already started */
	if (traceparent->sampled)
		goto cleanup;

	/* Don't start tracing if we're not at the root level */
	if (nested_level > 0)
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
			fetch_parallel_context(traceparent);
		goto cleanup;
	}

	Assert(traceid_zero(traceparent->trace_id));

	if (pstate != NULL)
		extract_trace_context_from_query(traceparent, pstate->p_sourcetext);

	/*
	 * A statement can go through this function 3 times: post parsing, planner
	 * and executor run. If no sampling flag was extracted from SQLCommenter
	 * and we've already seen this statement (using statement_start ts), don't
	 * try to apply sample rate and exit.
	 */
	statement_start_ts = GetCurrentStatementStartTimestamp();
	if (traceparent->sampled == 0
		&& last_statement_check_for_sampling == statement_start_ts)
		goto cleanup;
	/* First time we see this statement, save the time */
	last_statement_check_for_sampling = statement_start_ts;

	if (is_query_id_filtered(query_id))
		/* Query id filter is not matching, disable sampling */
		traceparent->sampled = 0;
	else
		traceparent->sampled = is_query_sampled(traceparent);

	if (traceparent->sampled && check_full_shared_spans())
		/* Buffer is full, abort sampling */
		traceparent->sampled = 0;

	if (traceparent->sampled)
		set_trace_id(traceparent);
	else
		/* No sampling, reset the context */
		reset_traceparent(traceparent);

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
	if (current_trace_spans == NULL)
		/* No need for cleaning */
		return;
	MemoryContextReset(pg_tracing_mem_ctx);

	/*
	 * Don't reset parse_traceparent here. With extended protocol +
	 * transaction block, tracing of the previous statement may end while
	 * parsing of the next statement was already done and stored in
	 * parse_traceparent.
	 */
	reset_traceparent(&executor_traceparent);
	within_declare_cursor = false;
	current_trace_spans = NULL;
	per_level_infos = NULL;
	cleanup_planstarts();
	cleanup_active_spans();
}

/*
 * Add span to the shared memory.
 * Exclusive lock on pg_tracing_shared_state->lock must be acquired beforehand.
 */
static void
add_span_to_shared_buffer_locked(const Span * span)
{
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
end_tracing(void)
{
	Size		file_position = 0;

	/* We're still a nested query, tracing is not finished */
	if (nested_level > 0)
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

	/* We can cleanup the rest here */
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
handle_pg_error(const pgTracingTraceparent * traceparent,
				const QueryDesc *queryDesc,
				TimestampTz span_end_time)
{
	int			sql_error_code;
	Span	   *span;

	sql_error_code = geterrcode();

	if (queryDesc != NULL)
		process_query_desc(traceparent, queryDesc, sql_error_code, span_end_time);
    span = peek_active_span();
	while (span != NULL)
	{
		/* Assign the error code to the latest top span */
		span->sql_error_code = sql_error_code;
		pop_active_span(&span_end_time);
        span = peek_active_span();
	};
}

/*
 * If we enter a new nested level, initialize all necessary buffers.
 */
static void
initialize_trace_level(void)
{
	/* Number of allocated nested levels. */
	static int	allocated_nested_level = 0;

	Assert(nested_level >= 0);

	/* First time */
	if (pg_tracing_mem_ctx->isReset)
	{
		MemoryContext oldcxt;

		Assert(pg_tracing_mem_ctx->isReset);

		oldcxt = MemoryContextSwitchTo(pg_tracing_mem_ctx);
		/* initial allocation */
		allocated_nested_level = 1;
		per_level_infos = palloc0(allocated_nested_level *
								  sizeof(pgTracingPerLevelInfos));
		current_trace_spans = palloc0(sizeof(pgTracingSpans) +
									  INITIAL_ALLOCATED_SPANS * sizeof(Span));
		current_trace_spans->max = INITIAL_ALLOCATED_SPANS;
		current_trace_text = makeStringInfo();

		MemoryContextSwitchTo(oldcxt);
	}
	else if (nested_level >= allocated_nested_level)
	{
		/* New nested level, allocate more memory */
		MemoryContext oldcxt;
		int			old_allocated_nested_level = allocated_nested_level;

		oldcxt = MemoryContextSwitchTo(pg_tracing_mem_ctx);
		allocated_nested_level++;
		per_level_infos = repalloc0(per_level_infos, old_allocated_nested_level * sizeof(pgTracingPerLevelInfos),
									allocated_nested_level * sizeof(pgTracingPerLevelInfos));
		MemoryContextSwitchTo(oldcxt);
	}
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
	pgTracingTraceparent *traceparent = &parse_traceparent;
	bool		new_lxid = is_new_lxid();
	bool		is_root_level = nested_level == 0;
	bool		new_local_transaction_statement;

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
		/* We're in a nested query, grab the ongoing traceparent */
		traceparent = &executor_traceparent;
	else
		/* At root level, we will start a new root span */
		reset_traceparent(&parse_traceparent);

	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query, jstate);

	/* If disabled, don't trace utility statement */
	if (query->utilityStmt && !pg_tracing_track_utility)
	{
		update_latest_lxid();
		return;
	}

	/* Evaluate if query is sampled or not */
	extract_trace_context(traceparent, pstate, query->queryId);
	new_local_transaction_statement = is_root_level && !new_lxid;

	if (!traceparent->sampled && (new_local_transaction_statement && tx_start_traceparent.sampled))
	{
		/*
		 * We're in the same traced local transaction, use the trace context
		 * extracted at the start of this transaction
		 */
		Assert(!traceid_zero(tx_start_traceparent.trace_id));
		*traceparent = tx_start_traceparent;
	}

	if (!traceparent->sampled)
	{
		/* Query is not sampled, nothing to do. */
		return;
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
	push_active_span(traceparent, command_type_to_span_type(query->commandType),
					 query, jstate, NULL,
					 pstate->p_sourcetext, start_top_span,
					 HOOK_PARSE, pg_tracing_export_parameters);
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
	pgTracingTraceparent *traceparent = &parse_traceparent;
	TimestampTz span_start_time;
	TimestampTz span_end_time;

	/*
	 * For nested planning (parse sampled and executor not sampled), we need
	 * to keep using parse_traceparent.
	 */
	bool		is_nested_planning = nested_level > 0 && !executor_traceparent.sampled && parse_traceparent.sampled;

	if (nested_level > 0 && !is_nested_planning)
	{
		/* We're in a nested query, grab the ongoing traceparent */
		traceparent = &executor_traceparent;
	}

	/* Evaluate if query is sampled or not */
	extract_trace_context(traceparent, NULL, query->queryId);

	if (!pg_tracing_enabled(traceparent, nested_level))
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
	push_active_span(traceparent, command_type_to_span_type(query->commandType),
					 query, NULL, NULL, query_string, span_start_time,
					 HOOK_PLANNER, pg_tracing_export_parameters);
	/* Create and start the planner span */
	push_child_active_span(traceparent, SPAN_PLANNER, query,
						   NULL, span_start_time);

	nested_level++;
	PG_TRY();
	{
		if (prev_planner_hook)
			result = prev_planner_hook(query, query_string, cursorOptions, params);
		else
			result = standard_planner(query, query_string, cursorOptions, params);
	}
	PG_CATCH();
	{
		nested_level--;
		span_end_time = GetCurrentTimestamp();
		handle_pg_error(traceparent, NULL, span_end_time);
		PG_RE_THROW();
	}
	PG_END_TRY();
	span_end_time = GetCurrentTimestamp();
	end_nested_level(&span_end_time);
	nested_level--;

	/* End planner span */
	pop_active_span(&span_end_time);

	/* If we have a prepared statement, add bound parameters to the top span */
	if (params != NULL && pg_tracing_export_parameters)
	{
		char	   *paramStr = BuildParamLogString(params, NULL,
												   pg_tracing_max_parameter_str);

		if (paramStr != NULL)
		{
			Span	   *span = peek_active_span();

			span->parameter_offset = add_str_to_trace_buffer(paramStr,
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
	pgTracingTraceparent *traceparent = &executor_traceparent;
	bool		is_lazy_function;
	TimestampTz start_span_time;

	if (nested_level == 0)
	{
		/* We're at the root level, copy parse traceparent */
		*traceparent = parse_traceparent;
		reset_traceparent(&parse_traceparent);
	}

	/*
	 * A cached plan will start here without going through parse and plan so
	 * we need to check trace_context here
	 */
	extract_trace_context(traceparent, NULL, queryDesc->plannedstmt->queryId);

	/*
	 * We can detect the presence of lazy function through the node tag and
	 * the executor flags. Lazy function will go through an ExecutorRun with
	 * every call, possibly generating thousands of spans (ex: lazy call of
	 * generate_series) which is not manageable and not very useful. If lazy
	 * functions are detected, we don't generate spans.
	 */
	is_lazy_function = nodeTag(queryDesc->plannedstmt->planTree) == T_FunctionScan
		&& eflags == EXEC_FLAG_SKIP_TRIGGERS;

	if (!pg_tracing_enabled(traceparent, nested_level) || is_lazy_function)
	{
		/* No sampling, go through normal ExecutorStart */
		if (prev_ExecutorStart)
			prev_ExecutorStart(queryDesc, eflags);
		else
			standard_ExecutorStart(queryDesc, eflags);
		return;
	}

	/* Executor is sampled */
	start_span_time = GetCurrentTimestamp();
	initialize_trace_level();

	/*
	 * In case of a cached plan, we haven't gone through neither parsing nor
	 * planner hook. Create the top case in this case.
	 */
	push_active_span(traceparent, command_type_to_span_type(queryDesc->operation),
					 NULL, NULL, queryDesc->plannedstmt,
					 queryDesc->sourceText, start_span_time, HOOK_EXECUTOR,
					 pg_tracing_export_parameters);

	/*
	 * We only need full instrumentation if we generate spans from planstate.
	 *
	 * If we're within a cursor declaration, we won't be able to generate
	 * spans from planstate due to cursor behaviour. Fetches will only call
	 * ExecutorRun and Closing the cursor will call ExecutorFinish and
	 * ExecutorEnd. We won't have the start of the planstate available and
	 * representing a node that has a fragmented execution doesn't fit our
	 * current model. So we disable full query instrumentation for cursors.
	 */
	if (pg_tracing_planstate_spans && !within_declare_cursor)
		queryDesc->instrument_options = INSTRUMENT_ALL;

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	/* Allocate totaltime instrumentation in the per-query context */
	if (queryDesc->totaltime == NULL)
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
	pgTracingTraceparent *traceparent = &executor_traceparent;
	TimestampTz span_start_time;
	TimestampTz span_end_time;
	Span	   *executor_run_span;

	if (!pg_tracing_enabled(traceparent, nested_level) || queryDesc->totaltime == NULL)
	{
		/* No sampling, go through normal execution */
		nested_level++;
		PG_TRY();
		{
			if (prev_ExecutorRun)
				prev_ExecutorRun(queryDesc, direction, count, execute_once);
			else
				standard_ExecutorRun(queryDesc, direction, count, execute_once);
		}
		PG_FINALLY();
		{
			nested_level--;
		}
		PG_END_TRY();
		return;
	}

	/* ExecutorRun is sampled */
	span_start_time = GetCurrentTimestamp();
	initialize_trace_level();

	/*
	 * When fetching an existing cursor, the portal already exists and
	 * ExecutorRun is the first hook called. Create the matching active span
	 * here.
	 */
	push_active_span(traceparent, command_type_to_span_type(queryDesc->operation), NULL,
					 NULL, queryDesc->plannedstmt, queryDesc->sourceText, span_start_time,
					 HOOK_EXECUTOR, pg_tracing_export_parameters);
	/* Start ExecutorRun span as a new active span */
	executor_run_span = push_child_active_span(traceparent, SPAN_EXECUTOR_RUN,
											   NULL, queryDesc->plannedstmt, span_start_time);
	per_level_infos[nested_level].executor_run_span_id = executor_run_span->span_id;
	per_level_infos[nested_level].executor_start = executor_run_span->start;

	/*
	 * If this query starts parallel worker, push the trace context for the
	 * child processes
	 */
	if (queryDesc->plannedstmt->parallelModeNeeded && pg_tracing_trace_parallel_workers)
		add_parallel_context(traceparent, executor_run_span->span_id);

	/*
	 * Setup ExecProcNode override to capture node start if planstate spans
	 * were requested. If there's no query instrumentation, we can skip
	 * ExecProcNode override.
	 */
	if (pg_tracing_planstate_spans && queryDesc->planstate->instrument)
		setup_ExecProcNode_override(queryDesc);

	nested_level++;
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
		if (current_trace_spans != NULL)
		{
			span_end_time = GetCurrentTimestamp();
			end_nested_level(&span_end_time);
			nested_level--;
			handle_pg_error(traceparent, queryDesc, span_end_time);
		}
		else
			nested_level--;
		remove_parallel_context();
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Remove parallel context if one was created */
	remove_parallel_context();

	/*
	 * Same as above, tracing could have been aborted, check for
	 * current_trace_spans
	 */
	if (current_trace_spans == NULL)
	{
		nested_level--;
		return;
	}

	span_end_time = GetCurrentTimestamp();
	end_nested_level(&span_end_time);
	nested_level--;
	/* End ExecutorRun span and store it */
	per_level_infos[nested_level].executor_end = span_end_time;
	pop_active_span(&span_end_time);
}

/*
 * ExecutorFinish hook: create executor finish span and track nesting depth.
 * ExecutorFinish can start nested queries through triggers so we need
 * to set the ExecutorFinish span as the top span.
 */
static void
pg_tracing_ExecutorFinish(QueryDesc *queryDesc)
{
	pgTracingTraceparent *traceparent = &executor_traceparent;
	TimestampTz span_start_time;
	TimestampTz span_end_time;
	int			num_stored_spans = 0;

	if (!pg_tracing_enabled(traceparent, nested_level)
		|| queryDesc->totaltime == NULL
		|| current_trace_spans == NULL)
	{
		/* No sampling or sampling was aborted */
		nested_level++;
		PG_TRY();
		{
			if (prev_ExecutorFinish)
				prev_ExecutorFinish(queryDesc);
			else
				standard_ExecutorFinish(queryDesc);
		}
		PG_FINALLY();
		{
			nested_level--;
		}
		PG_END_TRY();
		return;
	}

	/* Statement is sampled */
	span_start_time = GetCurrentTimestamp();
	initialize_trace_level();

	/*
	 * Save the initial number of spans for the current session. We will only
	 * store ExecutorFinish span if we have created nested spans.
	 */
	num_stored_spans = current_trace_spans->end;

	/*
	 * When closing a cursor, only ExecutorFinish and ExecutorEnd will be
	 * called. Create the top span in this case.
	 */
	push_active_span(traceparent, command_type_to_span_type(queryDesc->operation), NULL,
					 NULL, queryDesc->plannedstmt,
					 queryDesc->sourceText, span_start_time,
					 HOOK_EXECUTOR, pg_tracing_export_parameters);
	/* Create ExecutorFinish as a new potential top span */
	push_child_active_span(traceparent, SPAN_EXECUTOR_FINISH, NULL, queryDesc->plannedstmt,
						   span_start_time);
	if (nested_level == 0)
		/* Save the root query_id to be used by the xact hook */
		current_query_id = queryDesc->plannedstmt->queryId;

	nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_CATCH();
	{
		/*
		 * current_trace_spans may be NULL if an after trigger calls
		 * pg_tracing_consume_spans
		 */
		if (current_trace_spans == NULL)
			nested_level--;
		else
		{
			span_end_time = GetCurrentTimestamp();
			end_nested_level(&span_end_time);
			nested_level--;
			handle_pg_error(traceparent, queryDesc, span_end_time);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (current_trace_spans == NULL)
	{
		nested_level--;
		return;
	}

	/*
	 * We only trace executorFinish when it has a nested query, check if we
	 * have a possible child
	 */
	if (current_trace_spans->end > num_stored_spans)
	{
		span_end_time = GetCurrentTimestamp();
		end_nested_level(&span_end_time);
		pop_active_span(&span_end_time);
	}
	else
		pop_active_span(NULL);
	nested_level--;
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
	pgTracingTraceparent *traceparent = &executor_traceparent;
	TimestampTz parent_end;
	TimestampTz span_end_time;

	if (!pg_tracing_enabled(traceparent, nested_level) || queryDesc->totaltime == NULL)
	{
		if (prev_ExecutorEnd)
			prev_ExecutorEnd(queryDesc);
		else
			standard_ExecutorEnd(queryDesc);
		return;
	}

	parent_end = per_level_infos[nested_level].executor_end;
	process_query_desc(traceparent, queryDesc, 0, parent_end);
	drop_traced_planstate(nested_level);

	/* No need to increment nested level here */
	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	span_end_time = GetCurrentTimestamp();
	pop_active_span(&span_end_time);
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
	TimestampTz span_end_time;
	TimestampTz span_start_time;
	pgTracingTraceparent *traceparent = &executor_traceparent;
	bool		track_utility;
	bool		in_aborted_transaction;

	if (nested_level == 0)
	{
		/* We're at root level, copy the parse traceparent */
		*traceparent = parse_traceparent;
		reset_traceparent(&parse_traceparent);
	}

	if (!pg_tracing_enabled(traceparent, nested_level))
	{
		/* No sampling, just go through the standard process utility */
		nested_level++;
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv,
								dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree,
									context, params, queryEnv,
									dest, qc);
		nested_level--;
		return;
	}

	/* Statement is sampled */
	span_start_time = GetCurrentTimestamp();

	/*
	 * Keep track if we're in a declare cursor as we want to disable query
	 * instrumentation in this case.
	 */
	within_declare_cursor = nodeTag(pstmt->utilityStmt) == T_DeclareCursorStmt;

	/*
	 * Save track utility value since this value could be modified by a SET
	 * command
	 */
	track_utility = pg_tracing_track_utility;

	/*
	 * Save whether we're in an aborted transaction. A rollback will reset the
	 * state after standard_ProcessUtility
	 */
	in_aborted_transaction = IsAbortedTransactionBlockState();


	initialize_trace_level();
	if (track_utility)
	{
		push_active_span(traceparent, command_type_to_span_type(pstmt->commandType), NULL,
						 NULL, pstmt, queryString, span_start_time,
						 HOOK_EXECUTOR, pg_tracing_export_parameters);
		push_child_active_span(traceparent, SPAN_PROCESS_UTILITY, NULL, pstmt, span_start_time);
	}

	nested_level++;
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
			span_end_time = GetCurrentTimestamp();
			end_nested_level(&span_end_time);
			nested_level--;
			handle_pg_error(traceparent, NULL, span_end_time);
		}
		else
			nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Same as above, abort if tracing was disabled within process utility */
	if (current_trace_spans == NULL)
	{
		nested_level--;
		return;
	}
	span_end_time = GetCurrentTimestamp();
	end_nested_level(&span_end_time);
	nested_level--;

	/* Update process utility span with number of processed rows */
	if (qc != NULL)
	{
		Span	   *process_utility_span = peek_active_span();

		process_utility_span->node_counters.rows = qc->nprocessed;
	}

	if (nested_level == 0)
		current_query_id = pstmt->queryId;

	/* End ProcessUtility span and store it */
	pop_active_span(&span_end_time);
	/* Also end and store parent active span */
	pop_active_span(&span_end_time);

	/*
	 * If we're in an aborted transaction, xact callback won't be called so we
	 * need to end tracing here
	 */
	if (in_aborted_transaction)
		end_tracing();
}

/*
 * Handle xact callback events
 *
 * Create a commit span between pre-commit and commit event and end ongoing tracing
 */
static void
pg_tracing_xact_callback(XactEvent event, void *arg)
{
	pgTracingTraceparent *traceparent = &executor_traceparent;

	if (current_trace_spans == NULL)
		return;

	if (!executor_traceparent.sampled)
		traceparent = &parse_traceparent;

	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
			{
				/*
				 * Only create a commit span if there was a change (i.e. a xid
				 * was assigned). Commit on a read only transaction is mostly
				 * a no-op and would create more noise than anything
				 */
				bool		is_modifying_xact = MyProc->xid != InvalidTransactionId;

				if (traceparent->sampled && is_modifying_xact)
					begin_span(traceparent->trace_id, &commit_span,
							   SPAN_COMMIT, NULL, traceparent->parent_id,
							   current_query_id, NULL);
				break;
			}
		case XACT_EVENT_COMMIT:
			end_nested_level(NULL);
			if (commit_span.span_id > 0)
			{
				end_span(&commit_span, NULL);
				store_span(&commit_span);
			}

			end_tracing();
			reset_span(&commit_span);
			break;
		case XACT_EVENT_PARALLEL_COMMIT:
			end_nested_level(NULL);
			end_tracing();
			break;
		case XACT_EVENT_ABORT:
			/* TODO: Create an abort span */
			end_nested_level(NULL);
			end_tracing();
			reset_span(&commit_span);
			break;
		default:
			break;
	}
}
