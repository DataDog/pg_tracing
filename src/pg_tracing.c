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
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "pg_tracing.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/utility.h"
#include "utils/varlena.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC;

typedef enum
{
	PG_TRACING_TRACK_NONE,		/* track no statements */
	PG_TRACING_TRACK_TOP,		/* only top level statements */
	PG_TRACING_TRACK_ALL		/* all statements, including nested ones */
}			TrackingLevel;

typedef enum
{
	PG_TRACING_KEEP_ON_FULL,	/* Keep existing buffers when full */
	PG_TRACING_DROP_ON_FULL		/* Drop current buffers when full */
}			BufferMode;

/*
 * Structure to store Query id filtering array of query id used for filtering
 */
typedef struct QueryIdFilter
{
	int			num_query_id;	/* number of query ids */
	uint64		query_ids[FLEXIBLE_ARRAY_MEMBER];
}			QueryIdFilter;

/*
 * Structure to store per exec level informations
 */
typedef struct PerLevelInfos
{
	uint64		executor_run_span_id;	/* executor run span id for this
										 * level. Executor run is used as
										 * parent for spans generated from
										 * planstate */
	TimestampTz executor_run_start;
	TimestampTz executor_run_end;
}			PerLevelInfos;

/* GUC variables */
static int	pg_tracing_max_span;	/* Maximum number of spans to store */
static int	pg_tracing_shared_str_size; /* Size of the shared str area */
static int	pg_tracing_max_parameter_size;	/* Maximum size of the parameter
											 * str shared across a transaction */
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
static int	pg_tracing_track = PG_TRACING_TRACK_ALL;	/* tracking level */
static bool pg_tracing_track_utility = true;	/* whether to track utility
												 * commands */
static int	pg_tracing_buffer_mode = PG_TRACING_KEEP_ON_FULL;	/* behaviour on full
																 * buffer */
static char *pg_tracing_filter_query_ids = NULL;	/* only sample query
													 * matching query ids */
char	   *pg_tracing_otel_endpoint = NULL;	/* Otel collector to send
												 * spans to */
char	   *pg_tracing_otel_service_name = NULL;	/* Service name set in
													 * otel traces */
int			pg_tracing_otel_naptime;	/* Delay between upload of spans to
										 * otel collector */
int			pg_tracing_otel_connect_timeout_ms; /* Connect timeout to the otel
												 * collector */
static char *guc_tracecontext_str = NULL;	/* Trace context string propagated
											 * through GUC variable */

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
static MemoryContext pg_tracing_mem_ctx;

/* trace context at the root level of parse/planning hook */
static Traceparent parse_traceparent;

/* trace context set through GUC */
static Traceparent * guc_tracecontext;

/* trace context used in nested levels or within executor hooks */
static Traceparent executor_traceparent;

/* traceparent of the current transaction */
static Traceparent tx_traceparent;

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
 * Shared buffer for strings
 */
char	   *shared_str = NULL;

/*
 * Store spans for the current trace.
 * They will be added to shared_spans at the end of the query tracing.
 */
static pgTracingSpans * current_trace_spans;

/*
 * Stringinfo buffer used for operation_name
 */
static StringInfo operation_name_buffer;

/*
 * StringInfo buffer used for operation_name extracted from planstate
 */
static StringInfo plan_name_buffer;

/*
 * Stringinfo buffer used for deparse info and parameters
 */
static StringInfo parameters_buffer;

/* Current nesting depth of Planner+ExecutorRun+ProcessUtility calls */
int			nested_level = 0;

/* Whether we're in a cursor declaration */
static bool within_declare_cursor = false;

/* Commit span used in xact callbacks */
static Span commit_span;

/* Tx block span used to represent explicit transaction block */
static Span tx_block_span;

/* Candidate span end time for top span */
static TimestampTz top_span_end_candidate;

/*
 * query id at level 0 of the current tracing.
 * Used to assign queryId to the commit span
 */
static uint64 current_query_id;

static QueryIdFilter * query_id_filter = NULL;
static PerLevelInfos * per_level_infos = NULL;

/* Number of spans initially allocated at the start of a trace. */
#define	INITIAL_ALLOCATED_SPANS 25

/* Saved hook values in case of unload */
#if (PG_VERSION_NUM >= 150000)
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
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
#if (PG_VERSION_NUM < 180000)
static void pg_tracing_ExecutorRun(QueryDesc *queryDesc,
								   ScanDirection direction,
								   uint64 count, bool execute_once);
#else
static void pg_tracing_ExecutorRun(QueryDesc *queryDesc,
								   ScanDirection direction,
								   uint64 count);
#endif
static void pg_tracing_ExecutorFinish(QueryDesc *queryDesc);
static void pg_tracing_ExecutorEnd(QueryDesc *queryDesc);
static void pg_tracing_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
									  bool readOnlyTree,
									  ProcessUtilityContext context,
									  ParamListInfo params,
									  QueryEnvironment *queryEnv,
									  DestReceiver *dest, QueryCompletion *qc);
static void pg_tracing_xact_callback(XactEvent event, void *arg);

static void pg_tracing_shmem_request(void);
static void pg_tracing_shmem_startup_hook(void);
static Size pg_tracing_memsize(void);

static void initialize_trace_level(void);

static bool check_filter_query_ids(char **newval, void **extra, GucSource source);
static void assign_filter_query_ids(const char *newval, void *extra);
static bool check_guc_tracecontext(char **newval, void **extra, GucSource source);
static void assign_guc_tracecontext_hook(const char *newval, void *extra);

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
							"Maximum size of parameters shared across spans in the same transaction. 0 to disable parameters in span metadata.",
							NULL,
							&pg_tracing_max_parameter_size,
							4096,
							0,
							100000,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_tracing.shared_str_size",
							"Size of the allocated area in the shared memory used for spans' strings (operation_name, parameters, deparse infos...).",
							NULL,
							&pg_tracing_shared_str_size,
							5242880,
							0,
							52428800,
							PGC_POSTMASTER,
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

	DefineCustomIntVariable("pg_tracing.otel_naptime",
							"Duration between each upload of spans to the otel collector (in milliseconds).",
							NULL,
							&pg_tracing_otel_naptime,
							10000,
							1000,
							500000,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_tracing.otel_connect_timeout_ms",
							"Maximum time in milliseconds to connect to the otel collector.",
							NULL,
							&pg_tracing_otel_connect_timeout_ms,
							1000,
							100,
							600000,
							PGC_SIGHUP,
							0,
							NULL,
							&otel_config_int_assign_hook,
							NULL);

	DefineCustomStringVariable("pg_tracing.otel_endpoint",
							   "Otel endpoint to send spans.",
							   "If unset, no background worker to export to otel is created.",
							   &pg_tracing_otel_endpoint,
							   NULL,
							   PGC_SIGHUP,
							   0,
							   NULL,
							   &otel_config_string_assign_hook,
							   NULL);

	DefineCustomStringVariable("pg_tracing.otel_service_name",
							   "Service Name to set in traces sent to otel.",
							   NULL,
							   &pg_tracing_otel_service_name,
							   "PostgreSQL_Server",
							   PGC_SIGHUP,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pg_tracing.trace_context",
							   "Trace context propagated through GUC variable.",
							   NULL,
							   &guc_tracecontext_str,
							   NULL,
							   PGC_USERSET,
							   0,
							   check_guc_tracecontext,
							   assign_guc_tracecontext_hook,
							   NULL);


	/* For jumble state */
	EnableQueryId();

#if (PG_VERSION_NUM < 150000)
	pg_tracing_shmem_request();
#else
	MarkGUCPrefixReserved("pg_tracing");

	/* Install hooks. */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pg_tracing_shmem_request;
#endif

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pg_tracing_shmem_startup_hook;

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
	/* Start background worker */
	if (pg_tracing_otel_endpoint != NULL)
	{
		elog(INFO, "Starting otel exporter worker on endpoint %s", pg_tracing_otel_endpoint);
		pg_tracing_start_worker();
	}
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
	/* the parallel workers context  */
	size = add_size(size, mul_size(max_parallel_workers, sizeof(pgTracingParallelContext)));
	/* and the shared string */
	size = add_size(size, pg_tracing_shared_str_size);

	return size;
}

/*
 * shmem_startup hook: Call previous startup hook and initialise shmem
 */
static void
pg_tracing_shmem_startup_hook(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
	pg_tracing_shmem_startup();
}

/*
 * shmem_startup: allocate or attach to shared memory.
 */
void
pg_tracing_shmem_startup(void)
{
	bool		found_pg_tracing;
	bool		found_shared_spans;
	bool		found_shared_str;

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
	shared_str = ShmemInitStruct("PgTracing Shared str",
								 pg_tracing_shared_str_size,
								 &found_shared_str);

	/* Initialize pg_tracing memory context */
	pg_tracing_mem_ctx = AllocSetContextCreate(TopMemoryContext,
											   "pg_tracing memory context",
											   ALLOCSET_DEFAULT_SIZES);

	/* Initialize shmem for trace propagation to parallel workers */
	pg_tracing_shmem_parallel_startup();
	/* Initialize operation hash */
	init_operation_hash();

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
 * shmem_request hook: request additional shared resources.  We'll allocate
 * or attach to the shared resources in pg_tracing_shmem_startup().
 */
static void
pg_tracing_shmem_request(void)
{
#if (PG_VERSION_NUM >= 150000)
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif
	RequestAddinShmemSpace(pg_tracing_memsize());
	RequestNamedLWLockTranche("pg_tracing", 1);
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
	QueryIdFilter *result;
	QueryIdFilter *query_ids;
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

	size_query_id_filter = sizeof(QueryIdFilter) + list_length(queryidlist) * sizeof(uint64);
	/* Work on a palloced buffer */
	query_ids = (QueryIdFilter *) palloc(size_query_id_filter);

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
	result = (QueryIdFilter *) guc_malloc(LOG, size_query_id_filter);
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
	query_id_filter = (QueryIdFilter *) extra;
}

/*
 * Check hook for trace context guc parameter
 */
static bool
check_guc_tracecontext(char **newval, void **extra, GucSource source)
{
	char	   *input_trace_context = *newval;
	ParseTraceparentErr err;
	Traceparent *result;
	Traceparent parsed_traceparent;

	if (!input_trace_context || strcmp(input_trace_context, "") == 0)
	{
		/*
		 * Empty tracecontext, extra should already have been set to NULL so
		 * nothing to do
		 */
		return true;
	}

	/* Parse the parameter string */
	err = parse_trace_context(&parsed_traceparent, input_trace_context, strlen(input_trace_context));
	if (err != PARSE_OK)
	{
		GUC_check_errdetail("Error parsing tracecontext: %s", parse_code_to_err(err));
		return false;
	}

	/* We have a valid traceaprent, store it in a guc_malloced variable */
	result = (Traceparent *) guc_malloc(LOG, sizeof(Traceparent));
	if (result == NULL)
		return false;

	*result = parsed_traceparent;
	*extra = result;
	return true;
}

/*
 * Assign hook for trace context guc
 */
static void
assign_guc_tracecontext_hook(const char *newval, void *extra)
{
	/*
	 * We don't want to trace the SET query modifying the trace context so
	 * cancel ongoing trace.
	 */
	cleanup_tracing();

	/* Copy the parsed traceparent to our global guc_tracecontext */
	guc_tracecontext = (Traceparent *) extra;
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
 * Append str to shared_str
 * shared_state lock should be acquired
 */
Size
append_str_to_shared_str(const char *str, int str_len)
{
	Size		extent = pg_tracing_shared_state->extent;

	if (extent + str_len > pg_tracing_shared_str_size)
	{
		/* Not enough room in the shared_str buffer */
		pg_tracing_shared_state->stats.dropped_str++;
		return -1;
	}

	/* Copy str to the shared buffer */
	memcpy(shared_str + pg_tracing_shared_state->extent, str, str_len);
	/* Update our tracked extent */
	pg_tracing_shared_state->extent += str_len;
	return extent;
}

/*
 * Append a str to parameters_buffer.
 *
 * If the maximum defined by pg_tracing_max_parameter_size was reached,
 * buffer_full will be set to true.
 */
int
append_str_to_parameters_buffer(const char *str, int str_len, bool add_null)
{
	int			available_len = pg_tracing_max_parameter_size - parameters_buffer->len;

	if (available_len <= 0)
		return 0;

	if (str_len > available_len)
	{
		/* Not enough room, parameter will be truncated */
		appendBinaryStringInfo(parameters_buffer, str, available_len);
		appendBinaryStringInfo(parameters_buffer, "...\0", 4);
		return available_len;
	}

	/* We can copy the whole str */
	appendBinaryStringInfo(parameters_buffer, str, str_len);
	if (add_null)
		appendStringInfoChar(parameters_buffer, '\0');
	return str_len;
}

/*
 * Append param list from a prepared statement to the parameter_buffers
 */
static void
append_prepared_statements_parameters(Span * span, ParamListInfo params)
{
	int			bytes_written = 0;

	span->parameter_offset = parameters_buffer->len;

	for (int paramno = 0; paramno < params->numParams; paramno++)
	{
		ParamExternData *param = &params->params[paramno];

		if (param->isnull || !OidIsValid(param->ptype))
			bytes_written = append_str_to_parameters_buffer("\0", 1, false);
		else
		{
			Oid			typoutput;
			bool		typisvarlena;
			char	   *pstring;

			getTypeOutputInfo(param->ptype, &typoutput, &typisvarlena);
			pstring = OidOutputFunctionCall(typoutput, param->value);
			bytes_written = append_str_to_parameters_buffer(pstring, strlen(pstring), true);
		}
		if (bytes_written == 0)
		{
			span->num_truncated_parameters = params->numParams - span->num_parameters;
			return;
		}
		span->num_parameters++;
	}
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
#if PG_VERSION_NUM >= 150000
		case CMD_MERGE:
			return SPAN_TOP_MERGE;
#endif
		case CMD_UTILITY:
			return SPAN_TOP_UTILITY;
		case CMD_NOTHING:
			return SPAN_TOP_NOTHING;
		case CMD_UNKNOWN:
			return SPAN_TOP_UNKNOWN;
	}
	return SPAN_TOP_UNKNOWN;
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
 * Preallocate space in the shared current_trace_spans to have at least slots_needed slots
 */
static void
preallocate_spans(int slots_needed)
{
	int			target_size = current_trace_spans->end + slots_needed;

	Assert(slots_needed > 0);

	if (target_size > current_trace_spans->max)
	{
		current_trace_spans->max = target_size;
		current_trace_spans = repalloc(current_trace_spans, sizeof(pgTracingSpans) +
									   current_trace_spans->max * sizeof(Span));
	}
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
		/* Need to extend. */
		current_trace_spans->max *= 2;
		/* repalloc uses the pointer's memory context, no need to switch */
		current_trace_spans = repalloc(current_trace_spans, sizeof(pgTracingSpans) +
									   current_trace_spans->max * sizeof(Span));
	}
	current_trace_spans->spans[current_trace_spans->end++] = *span;
}

/*
 * Drop all spans from the shared buffer and reset shared str position.
 * Exclusive lock on pg_tracing->lock should be acquired beforehand.
 */
void
drop_all_spans_locked(void)
{
	/* Drop all spans */
	shared_spans->end = 0;
	/* Reset shared str position */
	pg_tracing_shared_state->extent = 0;
	/* Remove all hash entries */
	reset_operation_hash();
	/* Update last consume ts */
	pg_tracing_shared_state->stats.last_consume = GetCurrentTimestamp();
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
 * Process a query descriptor: Gather all query instrumentation in the query
 * span counters
 */
static void
process_query_desc(const Traceparent * traceparent, const QueryDesc *queryDesc,
				   int sql_error_code, bool deparse_plan, TimestampTz parent_end)
{
	NodeCounters *node_counters = &peek_active_span()->node_counters;

	if (!queryDesc->totaltime->running && queryDesc->totaltime->total == 0)
		return;

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

	if (pg_tracing_planstate_spans)
	{
		uint64		parent_id = per_level_infos[nested_level].executor_run_span_id;
		TimestampTz parent_start = per_level_infos[nested_level].executor_run_start;
		uint64		query_id = queryDesc->plannedstmt->queryId;

		process_planstate(traceparent, queryDesc, sql_error_code,
						  deparse_plan, parent_id, query_id,
						  parent_start, parent_end,
						  parameters_buffer, plan_name_buffer);
	}
}

/*
 * Create a trace id for the trace context if there's none.
 * If trace was started from the global sample rate without a parent trace, we
 * need to generate a random trace id.
 */
static void
set_trace_id(Traceparent * traceparent)
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
			tx_traceparent = *traceparent;
		return;
	}

	/*
	 * We want to keep the same trace id for all statements within the same
	 * transaction. For that, we check if we're in the same local xid and a
	 * trace_id was assigned to this transaction.
	 */
	if (!new_lxid && !traceid_zero(tx_traceparent.trace_id))
	{
		/* We're in the same transaction, use the transaction trace context */
		traceparent->trace_id = tx_traceparent.trace_id;
		return;
	}

	traceparent->trace_id.traceid_left = generate_rnd_uint64();
	traceparent->trace_id.traceid_right = generate_rnd_uint64();
	/* Tag it as generated */
	traceparent->generated = true;

	if (new_lxid)

		/*
		 * We're at the begining of a new local transaction, save trace
		 * context
		 */
		tx_traceparent = *traceparent;
	else

		/*
		 * We sampled a statement in a middle of a transaction, save the
		 * trace_id to reuse it if statements are sampled within the same
		 * transaction
		 */
		tx_traceparent.trace_id = traceparent->trace_id;
}

/*
 * Decide whether a query should be sampled depending on the traceparent sampled
 * flag and the provided sample rate configurations
 */
static bool
is_query_sampled(const Traceparent * traceparent)
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
	rand = generate_rnd_double();
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
extract_trace_context(Traceparent * traceparent, ParseState *pstate,
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

	/* We have a traceparent propagated through GUC, copy it */
	if (guc_tracecontext && guc_tracecontext->sampled)
	{
		*traceparent = *guc_tracecontext;
		goto cleanup;
	}

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
	if (!traceparent->sampled &&
		last_statement_check_for_sampling == statement_start_ts)
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
	/* Check if the current transaction is sampled use it if it's the case */
	if (!traceparent->sampled && tx_traceparent.sampled)
	{
		/* We have a sampled transaction, use the tx's traceparent */
		Assert(!traceid_zero(tx_traceparent.trace_id));
		*traceparent = tx_traceparent;
	}

	/* No matter what happens, we want to update the latest_lxid seen */
	update_latest_lxid();
}

/*
 * Reset traceparent fields
 */
void
reset_traceparent(Traceparent * traceparent)
{
	memset(traceparent, 0, sizeof(Traceparent));
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
	top_span_end_candidate = 0;
	cleanup_planstarts();
	cleanup_active_spans();
}

/*
 * End the query tracing and dump all spans in the shared buffer if we are at
 * the root level. This may happen either when query is finished or on a caught error.
 */
static void
end_tracing(void)
{
	Size		parameter_pos = 0;
	Size		plan_name_pos = 0;

	/* We're still a nested query, tracing is not finished */
	if (nested_level > 0)
		return;

	LWLockAcquire(pg_tracing_shared_state->lock, LW_EXCLUSIVE);

	if (parameters_buffer->len > 0)
		parameter_pos = append_str_to_shared_str(parameters_buffer->data, parameters_buffer->len);
	if (plan_name_buffer->len > 0)
		plan_name_pos = append_str_to_shared_str(plan_name_buffer->data, plan_name_buffer->len);

	/* We're at the end, add all stored spans to the shared memory */
	for (int i = 0; i < current_trace_spans->end; i++)
	{
		Span	   *span = &current_trace_spans->spans[i];

		if (span->operation_name_offset != -1)
		{
			if (span->type >= SPAN_TOP_SELECT && span->type <= SPAN_TOP_UNKNOWN)
				span->operation_name_offset = lookup_operation_name(span, operation_name_buffer->data + span->operation_name_offset);
			else
				span->operation_name_offset += plan_name_pos;
		}
		if (span->parameter_offset != -1)
			span->parameter_offset += parameter_pos;
		if (span->deparse_info_offset != -1)
			span->deparse_info_offset += parameter_pos;

		add_span_to_shared_buffer_locked(span);
	}

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
handle_pg_error(const Traceparent * traceparent,
				const QueryDesc *queryDesc,
				TimestampTz span_end_time)
{
	int			sql_error_code;
	Span	   *span;
	int			max_trace_spans PG_USED_FOR_ASSERTS_ONLY = current_trace_spans->max;

	sql_error_code = geterrcode();

	if (queryDesc != NULL)
	{
		/*
		 * On error, instrumentation may have been left running, stop it to
		 * avoid error thrown by InstrEndLoop
		 */
		if (!INSTR_TIME_IS_ZERO(queryDesc->totaltime->starttime))
			InstrStopNode(queryDesc->totaltime, 0);

		/*
		 * Within error, we need to avoid any possible allocation as this
		 * could be an out of memory error. deparse plan relies on allocation
		 * through lcons so we explicitely disable it.
		 */
		process_query_desc(traceparent, queryDesc, sql_error_code, false, span_end_time);
	}
	span = peek_active_span();
	while (span != NULL && span->nested_level == nested_level)
	{
		/* Assign the error code to the latest top span */
		span->sql_error_code = sql_error_code;
		/* End and store the span */
		pop_and_store_active_span(span_end_time);
		/* Get the next span in the stack */
		span = peek_active_span();
	};
	Assert(current_trace_spans->max == max_trace_spans);
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
								  sizeof(PerLevelInfos));
		current_trace_spans = palloc0(sizeof(pgTracingSpans) +
									  INITIAL_ALLOCATED_SPANS * sizeof(Span));
		current_trace_spans->max = INITIAL_ALLOCATED_SPANS;
		operation_name_buffer = makeStringInfo();
		plan_name_buffer = makeStringInfo();
		parameters_buffer = makeStringInfo();

		MemoryContextSwitchTo(oldcxt);
	}
	else if (nested_level >= allocated_nested_level)
	{
		/* New nested level, allocate more memory */
		int			old_allocated_nested_level = allocated_nested_level;

		allocated_nested_level++;
		/* repalloc uses the pointer's memory context, no need to switch */
		per_level_infos = repalloc0(per_level_infos, old_allocated_nested_level * sizeof(PerLevelInfos),
									allocated_nested_level * sizeof(PerLevelInfos));
	}
}

/*
 * End all spans for the current nested level
 */
static void
end_nested_level(TimestampTz span_end_time)
{
	Span	   *span;

	span = peek_active_span();
	if (span == NULL || span->nested_level < nested_level)
		return;

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

		pop_and_store_active_span(span_end_time);
		span = peek_active_span();
	}
}

/*
 * Adjust the tx_traceparent if the latest captured traceparent is more
 * relevant, i.e. provided from upstream and not automatically generated.
 */
static void
adjust_tx_traceparent(const Traceparent * traceparent)
{
	if (traceid_equal(traceparent->trace_id, tx_block_span.trace_id))
		return;
	if (!tx_traceparent.generated || traceparent->generated)
		return;

	/*
	 * We have a generated tx_traceparent and a provided traceparent, give
	 * priority to the provided traceparent and amend the existing spans
	 */
	tx_block_span.trace_id = traceparent->trace_id;
	tx_traceparent.trace_id = traceparent->trace_id;
	for (int i = 0; i < current_trace_spans->end; i++)
		current_trace_spans->spans[i].trace_id = traceparent->trace_id;
}

static void
initialise_span_context(SpanContext * span_context,
						Traceparent * traceparent,
						const PlannedStmt *pstmt,
						const JumbleState *jstate,
						Query *query, const char *query_text)
{
	Assert(pstmt || query);

	span_context->start_time = GetCurrentTimestamp();
	span_context->traceparent = traceparent;
	if (!traceid_zero(tx_block_span.trace_id) && nested_level == 0)
	{
		adjust_tx_traceparent(span_context->traceparent);
		/* We have an ongoing transaction block. Use it as parent for level 0 */
		span_context->traceparent->parent_id = tx_block_span.span_id;
	}
	span_context->parameters_buffer = parameters_buffer;
	span_context->operation_name_buffer = operation_name_buffer;
	span_context->pstmt = pstmt;
	span_context->query = query;
	span_context->jstate = jstate;
	span_context->query_text = query_text;
	span_context->max_parameter_size = pg_tracing_max_parameter_size;

	if (span_context->pstmt)
		span_context->query_id = pstmt->queryId;
	else
		span_context->query_id = query->queryId;
}

static bool
should_start_tx_block(const Node *utilityStmt, HookType hook_type)
{
	TransactionStmt *stmt;

	if (hook_type == HOOK_PARSE && GetCurrentTransactionStartTimestamp() != GetCurrentStatementStartTimestamp())
		/* There's an ongoing tx block, we can create the matching span */
		return true;
	if (hook_type == HOOK_UTILITY && utilityStmt != NULL && nodeTag(utilityStmt) == T_TransactionStmt)
	{
		stmt = (TransactionStmt *) utilityStmt;
		/* If we have an explicit BEGIN statement, start a tx block */
		return (stmt->kind == TRANS_STMT_BEGIN);
	}
	return false;
}

/*
 * Start a tx_block span if we have an explicit BEGIN utility statement
 */
static void
start_tx_block_span(const Node *utilityStmt, SpanContext * span_context, HookType hook_type)
{
	if (nested_level > 0)
		return;

	if (tx_block_span.span_id > 0)
		/* We already have an ongoing tx_block span */
		return;

	if (!should_start_tx_block(utilityStmt, hook_type))
		/* Not a candidate to start tx block */
		return;

	/*
	 * ProcessUtility may start a new transaction with a BEGIN. We want to
	 * prevent tracing to be cleared if next parse doesn't have the same lxid
	 * so we udpate the latest_lxid observed.
	 */
	update_latest_lxid();

	/* We have an explicit BEGIN, start a transaction block span */
	begin_span(span_context->traceparent->trace_id, &tx_block_span,
			   SPAN_TRANSACTION_BLOCK, NULL, span_context->traceparent->parent_id,
			   span_context->query_id, GetCurrentTransactionStartTimestamp());
	/* Update traceparent context to use the tx_block span as parent */
	span_context->traceparent->parent_id = tx_block_span.span_id;
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
	bool		new_lxid = is_new_lxid();
	bool		is_root_level = nested_level == 0;
	Traceparent *traceparent = &parse_traceparent;
	SpanContext span_context;

	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query, jstate);

	if (new_lxid)
		/* We have a new local transaction, reset the tx_start traceparent */
		reset_traceparent(&tx_traceparent);

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
		/* We're in a nested query, grab the ongoing executor traceparent */
		traceparent = &executor_traceparent;
	else
		/* We're at root level, reset the parse traceparent */
		reset_traceparent(&parse_traceparent);

	/* If disabled, don't trace utility statement */
	if (query->utilityStmt && !pg_tracing_track_utility)
	{
		update_latest_lxid();
		return;
	}

	/* Evaluate if query is sampled or not */
	extract_trace_context(traceparent, pstate, query->queryId);

	/*
	 * Keep track of the start of the last parse at root level. When explicit
	 * tx is used with extended protocol, the end of the previous command
	 * happens when the next command is bound, creating overlapping spans. To
	 * avoid this, we want to end the previous command as soon as the new
	 * command is parsed or bound
	 */
	if (nested_level == 0)
		top_span_end_candidate = GetCurrentStatementStartTimestamp();

	if (!traceparent->sampled)
		/* Query is not sampled, nothing to do. */
		return;

	/* Statement is sampled, initialize memory and push a new active span */
	initialize_trace_level();
	initialise_span_context(&span_context, traceparent,
							NULL, jstate, query, pstate->p_sourcetext);

	/*
	 * With queries using simple protocol, post parse is a good place to start
	 * tx_block span
	 */
	start_tx_block_span(query->utilityStmt, &span_context, HOOK_PARSE);

	push_active_span(pg_tracing_mem_ctx, &span_context, command_type_to_span_type(query->commandType),
					 HOOK_PARSE);
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
	SpanContext span_context;
	PlannedStmt *result;
	TimestampTz span_end_time;
	Traceparent *traceparent = &parse_traceparent;

	/*
	 * For nested planning (parse sampled and executor not sampled), we need
	 * to use parse_traceparent.
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
	initialize_trace_level();
	initialise_span_context(&span_context, traceparent,
							NULL, NULL, query, query_string);
	push_active_span(pg_tracing_mem_ctx, &span_context, command_type_to_span_type(query->commandType),
					 HOOK_PLANNER);
	/* Create and start the planner span */
	push_child_active_span(pg_tracing_mem_ctx, &span_context, SPAN_PLANNER);

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
	end_nested_level(span_end_time);
	nested_level--;

	/* End planner span */
	pop_and_store_active_span(span_end_time);

	/* If we have a prepared statement, add bound parameters to the top span */
	if (pg_tracing_max_parameter_size &&
		!IsAbortedTransactionBlockState() &&
		params != NULL &&
		params->paramFetch == NULL &&
		params->numParams > 0)
	{
		Span	   *span = peek_active_span();

		append_prepared_statements_parameters(span, params);
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
	SpanContext span_context;
	bool		is_lazy_function;
	Traceparent *traceparent = &executor_traceparent;

	if (nested_level == 0)
	{
		/* We're at the root level, copy parse traceparent */
		*traceparent = parse_traceparent;
		reset_traceparent(&parse_traceparent);

		/*
		 * Reset top span end candidate, either a new parse will set a new
		 * value if extended protocol is used or we will just use the current
		 * ts in ExecutorEnd
		 */
		top_span_end_candidate = 0;
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
			return prev_ExecutorStart(queryDesc, eflags);
		else
			return standard_ExecutorStart(queryDesc, eflags);
	}

	/* Statement is sampled */
	initialize_trace_level();
	initialise_span_context(&span_context, traceparent,
							queryDesc->plannedstmt, NULL, NULL, queryDesc->sourceText);
	push_active_span(pg_tracing_mem_ctx, &span_context, command_type_to_span_type(queryDesc->operation),
					 HOOK_EXECUTOR);

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
#if (PG_VERSION_NUM < 180000)
pg_tracing_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
					   bool execute_once)
#else
pg_tracing_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
#endif
{
	SpanContext span_context;
	TimestampTz span_end_time;
	Span	   *executor_run_span;
	Traceparent *traceparent = &executor_traceparent;
	int			num_nodes;
	uint64		parallel_workers_parent_id = 0;

	if (!pg_tracing_enabled(traceparent, nested_level) || queryDesc->totaltime == NULL)
	{
		/* No sampling, go through normal execution */
		nested_level++;
		PG_TRY();
		{
			EXECUTOR_RUN();
		}
		PG_FINALLY();
		{
			nested_level--;
		}
		PG_END_TRY();
		return;
	}

	/*
	 * Temporary workaround around a known issue where query id is not
	 * propagated with extended protocol. See
	 * https://www.postgresql.org/message-id/flat/CA+427g8DiW3aZ6pOpVgkPbqK97ouBdf18VLiHFesea2jUk3XoQ@mail.gmail.com
	 */
	pgstat_report_query_id(queryDesc->plannedstmt->queryId, false);

	/* ExecutorRun is sampled */
	initialize_trace_level();
	initialise_span_context(&span_context, traceparent,
							queryDesc->plannedstmt, NULL, NULL, queryDesc->sourceText);
	push_active_span(pg_tracing_mem_ctx, &span_context, command_type_to_span_type(queryDesc->operation),
					 HOOK_EXECUTOR);
	/* Start ExecutorRun span as a new active span */
	executor_run_span = push_child_active_span(pg_tracing_mem_ctx, &span_context, SPAN_EXECUTOR_RUN);

	per_level_infos[nested_level].executor_run_span_id = executor_run_span->span_id;
	per_level_infos[nested_level].executor_run_start = executor_run_span->start;

	/*
	 * If this query starts parallel worker, push the trace context for the
	 * child processes
	 */
	if (queryDesc->plannedstmt->parallelModeNeeded && pg_tracing_trace_parallel_workers)
	{
		parallel_workers_parent_id = generate_parallel_workers_parent_id();
		add_parallel_context(traceparent, parallel_workers_parent_id);
	}

	/*
	 * Setup ExecProcNode override to capture node start if planstate spans
	 * were requested. If there's no query instrumentation, we can skip
	 * ExecProcNode override.
	 */
	if (pg_tracing_planstate_spans && queryDesc->planstate->instrument)
		setup_ExecProcNode_override(pg_tracing_mem_ctx, queryDesc);

	/*
	 * Preallocate enough space in the current_trace_spans to process the
	 * queryDesc in the error handler without doing new allocations
	 */
	num_nodes = number_nodes_from_planstate(queryDesc->planstate);
	preallocate_spans(num_nodes);

	nested_level++;
	PG_TRY();
	{
		EXECUTOR_RUN();
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
			end_nested_level(span_end_time);
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

	/* End nested level */
	span_end_time = GetCurrentTimestamp();
	end_nested_level(span_end_time);
	nested_level--;
	/* End ExecutorRun span and store it */
	pop_and_store_active_span(span_end_time);
	per_level_infos[nested_level].executor_run_end = span_end_time;
}

/*
 * ExecutorFinish hook: create executor finish span and track nesting depth.
 * ExecutorFinish can start nested queries through triggers so we need
 * to set the ExecutorFinish span as the top span.
 */
static void
pg_tracing_ExecutorFinish(QueryDesc *queryDesc)
{
	SpanContext span_context;
	TimestampTz span_end_time;
	int			num_stored_spans = 0;
	Traceparent *traceparent = &executor_traceparent;

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
	initialize_trace_level();
	initialise_span_context(&span_context, traceparent,
							queryDesc->plannedstmt, NULL, NULL, queryDesc->sourceText);
	push_active_span(pg_tracing_mem_ctx, &span_context, command_type_to_span_type(queryDesc->operation),
					 HOOK_EXECUTOR);
	/* Create ExecutorFinish as a new potential top span */
	push_child_active_span(pg_tracing_mem_ctx, &span_context, SPAN_EXECUTOR_FINISH);

	/*
	 * Save the initial number of spans for the current session. We will only
	 * store ExecutorFinish span if we have created nested spans.
	 */
	num_stored_spans = current_trace_spans->end;

	if (nested_level == 0)
		/* Save the root query_id to be used by the xact commit hook */
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
			end_nested_level(span_end_time);
			nested_level--;
			handle_pg_error(traceparent, queryDesc, span_end_time);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Tracing may have been aborted, check and bail out if it's the case */
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
		end_nested_level(span_end_time);
		pop_and_store_active_span(span_end_time);
	}
	else
		pop_active_span();
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
	TimestampTz parent_end;
	TimestampTz span_end_time;
	Traceparent *traceparent = &executor_traceparent;

	if (!pg_tracing_enabled(traceparent, nested_level) || queryDesc->totaltime == NULL)
	{
		if (prev_ExecutorEnd)
			prev_ExecutorEnd(queryDesc);
		else
			standard_ExecutorEnd(queryDesc);
		return;
	}

	/*
	 * We're at the end of the Executor for this level, generate spans from
	 * the planstate if needed
	 */
	parent_end = per_level_infos[nested_level].executor_run_end;
	process_query_desc(traceparent, queryDesc, 0, pg_tracing_deparse_plan, parent_end);

	/* No need to increment nested level here */
	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	if (nested_level == 0 && top_span_end_candidate > 0)
		span_end_time = top_span_end_candidate;
	else
		span_end_time = GetCurrentTimestamp();
	pop_and_store_active_span(span_end_time);
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
	SpanContext span_context;
	bool		track_utility;
	bool		in_aborted_transaction;
	Traceparent *traceparent = &executor_traceparent;

	if (nested_level == 0)
	{
		/* We're at root level, copy the parse traceparent */
		*traceparent = parse_traceparent;
		reset_traceparent(&parse_traceparent);

		/* Update tracked query_id */
		current_query_id = pstmt->queryId;
	}

	/* Use tx_traceparent if the whole tx is traced */
	if (!traceparent->sampled && tx_traceparent.sampled)
		*traceparent = tx_traceparent;

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

	/*
	 * Temporary workaround around a known issue where query id is not
	 * propagated with extended protocol. See
	 * https://www.postgresql.org/message-id/flat/CA+427g8DiW3aZ6pOpVgkPbqK97ouBdf18VLiHFesea2jUk3XoQ@mail.gmail.com
	 */
	pgstat_report_query_id(pstmt->queryId, false);

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
	initialise_span_context(&span_context, traceparent,
							pstmt, NULL, NULL, queryString);
	start_tx_block_span(pstmt->utilityStmt, &span_context, HOOK_UTILITY);

	if (track_utility)
	{
		push_active_span(pg_tracing_mem_ctx, &span_context, command_type_to_span_type(pstmt->commandType),
						 HOOK_EXECUTOR);
		push_child_active_span(pg_tracing_mem_ctx, &span_context, SPAN_PROCESS_UTILITY);
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
			end_nested_level(span_end_time);
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
	end_nested_level(span_end_time);
	nested_level--;

	/* Update process utility span with number of processed rows */
	if (qc != NULL)
	{
		Span	   *process_utility_span = peek_active_span();

		process_utility_span->node_counters.rows = qc->nprocessed;
	}

	/* End ProcessUtility span and store it */
	pop_and_store_active_span(span_end_time);

	if (nested_level == 0 && tx_block_span.span_id > 0)
	{
		/*
		 * Parent span may have been created during post parse and tx block
		 * started during process utility. Link the parent span to the tx
		 * block here
		 */
		Span	   *parent_span = peek_active_span();

		Assert(parent_span->nested_level == 0);
		parent_span->parent_id = tx_block_span.span_id;
	}
	/* Also end and store parent active span */
	pop_and_store_active_span(span_end_time);

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
	Traceparent *traceparent = &executor_traceparent;
	TimestampTz current_ts;

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
				{
					current_ts = GetCurrentTimestamp();

					begin_span(traceparent->trace_id, &commit_span,
							   SPAN_TRANSACTION_COMMIT, NULL, traceparent->parent_id,
							   current_query_id, current_ts);
				}
				break;
			}
		case XACT_EVENT_ABORT:
		case XACT_EVENT_COMMIT:
			current_ts = GetCurrentTimestamp();
			end_nested_level(current_ts);
			if (commit_span.span_id > 0)
			{
				end_span(&commit_span, &current_ts);
				store_span(&commit_span);
			}
			if (tx_block_span.span_id > 0)
			{
				/*
				 * We don't use GetCurrentTransactionStopTimestamp as that
				 * would make the commit span overlaps and ends after the
				 * TransactionBlock
				 */
				end_span(&tx_block_span, &current_ts);
				store_span(&tx_block_span);
			}

			end_tracing();
			reset_span(&commit_span);
			reset_span(&tx_block_span);
			break;
		case XACT_EVENT_PARALLEL_COMMIT:
			current_ts = GetCurrentTimestamp();
			end_nested_level(current_ts);
			end_tracing();
			break;
		default:
			break;
	}
}
