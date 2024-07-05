/*-------------------------------------------------------------------------
 * pg_tracing.h
 * 		Header for pg_tracing.
 *
 * IDENTIFICATION
 *	  src/pg_tracing.h
 *-------------------------------------------------------------------------
 */
#ifndef _PG_TRACING_H_
#define _PG_TRACING_H_

#include "executor/execdesc.h"
#include "jit/jit.h"
#include "pgstat.h"
#include "storage/lwlock.h"
#include "version_compat.h"

/* Location of external text file */
#define PG_TRACING_TEXT_FILE	PG_STAT_TMP_DIR "/pg_tracing.stat"
#define INT64_HEX_FORMAT "%016" INT64_MODIFIER "x"

/*
 * Counters extracted from query instrumentation
 */
typedef struct NodeCounters
{
	int64		rows;			/* # of tuples processed */
	int64		nloops;			/* # of cycles for this node */
	BufferUsage buffer_usage;	/* total buffer usage for this node */
	WalUsage	wal_usage;		/* total WAL usage for this node */
	JitInstrumentation jit_usage;	/* total JIT usage for this node */
}			NodeCounters;

/*
 * Counters extracted from query's plan
 */
typedef struct PlanCounters
{
	/* estimated execution costs for plan (see costsize.c for more info) */
	Cost		startup_cost;	/* cost expended before fetching any tuples */
	Cost		total_cost;		/* total cost (assuming all tuples fetched) */

	/* planner's estimate of result size of this plan step */
	Cardinality plan_rows;		/* number of rows plan is expected to emit */
	int			plan_width;		/* average row width in bytes */
}			PlanCounters;

/*
 * Hook currently called
 */
typedef enum HookPhase
{
	HOOK_PARSE,
	HOOK_PLANNER,
	HOOK_EXECUTOR,
}			HookPhase;

/*
 * Error code when parsing traceparent field
 */
typedef enum ParseTraceparentErr
{
	PARSE_OK = 0,
	PARSE_INCORRECT_SIZE,
	PARSE_NO_TRACEPARENT_FIELD,
	PARSE_INCORRECT_TRACEPARENT_SIZE,
	PARSE_INCORRECT_FORMAT,
}			ParseTraceparentErr;

/*
 * SpanType: Type of the span
 */
typedef enum SpanType
{
	SPAN_PLANNER,				/* Wraps planner execution in planner hook */
	SPAN_FUNCTION,				/* Wraps function in fmgr hook */
	SPAN_PROCESS_UTILITY,		/* Wraps ProcessUtility execution */

	SPAN_EXECUTOR_RUN,			/* Wraps Executor run hook */
	SPAN_EXECUTOR_FINISH,		/* Wraps Executor finish hook */
	SPAN_TRANSACTION_COMMIT,	/* Wraps time between pre-commit and commit */

	/* Represents a node execution, generated from planstate */
	SPAN_NODE,
	SPAN_NODE_RESULT,
	SPAN_NODE_PROJECT_SET,

	/* Modify table */
	SPAN_NODE_INSERT,
	SPAN_NODE_UPDATE,
	SPAN_NODE_DELETE,
	SPAN_NODE_MERGE,

	SPAN_NODE_APPEND,
	SPAN_NODE_MERGE_APPEND,
	SPAN_NODE_RECURSIVE_UNION,
	SPAN_NODE_BITMAP_AND,
	SPAN_NODE_BITMAP_OR,
	SPAN_NODE_NESTLOOP,
	SPAN_NODE_MERGE_JOIN,
	SPAN_NODE_HASH_JOIN,
	SPAN_NODE_SEQ_SCAN,
	SPAN_NODE_SAMPLE_SCAN,
	SPAN_NODE_GATHER,
	SPAN_NODE_GATHER_MERGE,
	SPAN_NODE_INDEX_SCAN,
	SPAN_NODE_INDEX_ONLY_SCAN,
	SPAN_NODE_BITMAP_INDEX_SCAN,
	SPAN_NODE_BITMAP_HEAP_SCAN,
	SPAN_NODE_TID_SCAN,
	SPAN_NODE_TID_RANGE_SCAN,
	SPAN_NODE_SUBQUERY_SCAN,
	SPAN_NODE_FUNCTION_SCAN,
	SPAN_NODE_TABLEFUNC_SCAN,
	SPAN_NODE_VALUES_SCAN,
	SPAN_NODE_CTE_SCAN,
	SPAN_NODE_NAMED_TUPLE_STORE_SCAN,
	SPAN_NODE_WORKTABLE_SCAN,

	SPAN_NODE_FOREIGN_SCAN,
	SPAN_NODE_FOREIGN_INSERT,
	SPAN_NODE_FOREIGN_UPDATE,
	SPAN_NODE_FOREIGN_DELETE,

	SPAN_NODE_CUSTOM_SCAN,
	SPAN_NODE_MATERIALIZE,
	SPAN_NODE_MEMOIZE,
	SPAN_NODE_SORT,
	SPAN_NODE_INCREMENTAL_SORT,
	SPAN_NODE_GROUP,

	SPAN_NODE_AGGREGATE,
	SPAN_NODE_GROUP_AGGREGATE,
	SPAN_NODE_HASH_AGGREGATE,
	SPAN_NODE_MIXED_AGGREGATE,

	SPAN_NODE_WINDOW_AGG,
	SPAN_NODE_UNIQUE,

	SPAN_NODE_SETOP,
	SPAN_NODE_SETOP_HASHED,

	SPAN_NODE_LOCK_ROWS,
	SPAN_NODE_LIMIT,
	SPAN_NODE_HASH,

	SPAN_NODE_INIT_PLAN,
	SPAN_NODE_SUBPLAN,

	SPAN_NODE_UNKNOWN,

	/* Top Span types. They are created from the query cmdType */
	SPAN_TOP_SELECT,
	SPAN_TOP_INSERT,
	SPAN_TOP_UPDATE,
	SPAN_TOP_DELETE,
	SPAN_TOP_MERGE,
	SPAN_TOP_UTILITY,
	SPAN_TOP_NOTHING,
	SPAN_TOP_UNKNOWN,

	NUM_SPAN_TYPE,				/* Must be last! */
}			SpanType;


/*
 * In standard, traceId is defined as a 16 bytes array propagated
 * with 32 hexadecimal characters. We store it as 2 int64 for simplicity
 * and to reduce memory footprint.
*/
typedef struct
{
	uint64		traceid_left;
	uint64		traceid_right;
}			TraceId;


/*
 * The Span data structure represents an operation with a start, a duration
 * and metadatas.
 */
typedef struct Span
{
	TraceId		trace_id;		/* Trace id extracted from the SQLCommenter's
								 * traceparent */
	uint64		span_id;		/* Span Identifier generated from a random
								 * uint64 */
	uint64		parent_id;		/* Span's parent id. For the top span, it's
								 * extracted from SQLCommenter's traceparent.
								 * For other spans, we pass the parent's span */
	uint64		query_id;		/* QueryId of the trace query if available */

	TimestampTz start;			/* Start of the span */
	TimestampTz end;			/* End of the span */

	SpanType	type;			/* Type of the span. Used to generate the
								 * span's name */

	bool		parallel_aware;
	bool		async_capable;

	int8		nested_level;	/* Nested level of this span this span.
								 * Internal usage only */
	int8		parent_planstate_index; /* Index to the parent planstate of
										 * this span. Internal usage only */
	uint8		subxact_count;	/* Active count of backend's subtransaction */

	uint16		num_parameters; /* Number of parameters */

	int			be_pid;			/* Pid of the backend process */
	Oid			user_id;		/* User ID when the span was created */
	Oid			database_id;	/* Database ID where the span was created */
	int			worker_id;		/* Worker id */

	/*
	 * We store variable size metadata in an external file. Those represent
	 * the position of NULL terminated strings in the file. Set to -1 if
	 * unused.
	 */
	Size		operation_name_offset;	/* operation represented by the span */
	Size		parameter_offset;	/* query parameters values */
	Size		deparse_info_offset;	/* info from deparsed plan */

	PlanCounters plan_counters; /* Counters with plan costs */
	NodeCounters node_counters; /* Counters with node costs (jit, wal,
								 * buffers) */
	int64		startup;		/* Time to the first tuple in nanoseconds */
	int			sql_error_code; /* query error code extracted from ErrorData,
								 * 0 if query was successful */
}			Span;


/*
 * Global statistics for pg_tracing
 */
typedef struct pgTracingStats
{
	int64		processed_traces;	/* number of traces processed */
	int64		processed_spans;	/* number of spans processed */
	int64		dropped_traces; /* number of traces aborted due to full buffer */
	int64		dropped_spans;	/* number of dropped spans due to full buffer */
	int64		otel_sent_spans;	/* number of spans sent to otel collector */
	int64		otel_failures;	/* number of failures to send spans to otel
								 * collector */
	TimestampTz last_consume;	/* Last time the shared spans buffer was
								 * consumed */
	TimestampTz stats_reset;	/* Last time stats were reset */
}			pgTracingStats;

/*
 * Global shared state
 */
typedef struct pgTracingSharedState
{
	LWLock	   *lock;			/* protects shared spans, shared state and
								 * query file */
	Size		extent;			/* current extent of query file */
	pgTracingStats stats;		/* global statistics for pg_tracing */
}			pgTracingSharedState;

/* Context needed when generating spans from planstate */
typedef struct planstateTraceContext
{
	TraceId		trace_id;
	int			sql_error_code;
	List	   *ancestors;
	List	   *deparse_ctx;
	List	   *rtable_names;
}			planstateTraceContext;

/*
 * Traceparent values propagated by the caller
 */
typedef struct Traceparent
{
	TraceId		trace_id;		/* Id of the trace */
	uint64		parent_id;		/* Span id of the parent */
	int			sampled;		/* Is current statement sampled? */
}			Traceparent;

/*
 * A trace context for a specific parallel context
 */
typedef struct pgTracingParallelContext
{
	ProcNumber	leader_backend_id;	/* Backend id of the leader, set to
									 * InvalidBackendId if unused */
	Traceparent traceparent;
}			pgTracingParallelContext;

/*
 * Store context for parallel workers
 */
typedef struct pgTracingParallelWorkers
{
	slock_t		mutex;
	pgTracingParallelContext trace_contexts[FLEXIBLE_ARRAY_MEMBER];
}			pgTracingParallelWorkers;

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
 * Match a planstate to the first start of a node.
 * This is needed to set the start for spans generated from planstate.
 */
typedef struct TracedPlanstate
{
	PlanState  *planstate;
	TimestampTz node_start;
	uint64		span_id;
	int			nested_level;
}			TracedPlanstate;

/*
 * Store context to marshal spans to json
 */
typedef struct JsonContext
{
	StringInfo	str;
	const char *qbuffer;
	Size		qbuffer_size;
	int			span_type_count[NUM_SPAN_TYPE];
	const		Span **span_type_to_spans[NUM_SPAN_TYPE];
}			JsonContext;

typedef struct SpanContext
{
	TimestampTz start_time;
	Traceparent *traceparent;
	StringInfo	current_trace_text;
	const PlannedStmt *pstmt;
	const Query *query;
	const JumbleState *jstate;
	const char *query_text;
	uint64		query_id;
	bool		export_parameters;
}			SpanContext;

/* pg_tracing_explain.c */
extern SpanType plan_to_span_type(const Plan *plan);
extern const char *plan_to_rel_name(const planstateTraceContext * planstateTraceContext, const PlanState *planstate);
extern const char *plan_to_deparse_info(const planstateTraceContext * planstateTraceContext, const PlanState *planstate);

/* pg_tracing_parallel.c */
extern void pg_tracing_shmem_parallel_startup(void);
extern void add_parallel_context(const Traceparent * traceparent, uint64 parent_id);
extern void remove_parallel_context(void);
extern void fetch_parallel_context(Traceparent * traceparent);

/* pg_tracing_planstate.c */

extern void
			process_planstate(const Traceparent * traceparent, const QueryDesc *queryDesc,
							  int sql_error_code, bool deparse_plan, uint64 parent_id,
							  TimestampTz parent_start, TimestampTz parent_end);

extern void
			setup_ExecProcNode_override(MemoryContext context, QueryDesc *queryDesc);
extern void
			cleanup_planstarts(void);
extern TracedPlanstate *
get_traced_planstate_from_index(int index);
extern int
			get_parent_traced_planstate_index(int nested_level);
extern TimestampTz
			get_span_end_from_planstate(PlanState *planstate, TimestampTz plan_start, TimestampTz root_end);

/* pg_tracing_query_process.c */
extern const char *normalise_query_parameters(const JumbleState *jstate, const char *query,
											  int query_loc, int *query_len_p, StringInfo trace_text,
											  int *num_parameters);
extern void extract_trace_context_from_query(Traceparent * traceparent, const char *query);
extern ParseTraceparentErr parse_trace_context(Traceparent * traceparent, const char *trace_context_str, int trace_context_len);
extern char *parse_code_to_err(ParseTraceparentErr err);
extern const char *normalise_query(const char *query, int query_loc, int *query_len_p);
extern bool text_store_file(pgTracingSharedState * pg_tracing, const char *query,
							int query_len, Size *query_offset);
extern char *qtext_load_file(Size *buffer_size);

/* pg_tracing_span.c */
extern void begin_span(TraceId trace_id, Span * span, SpanType type,
					   const uint64 *span_id, uint64 parent_id, uint64 query_id,
					   TimestampTz start_span);
extern void end_span(Span * span, const TimestampTz *end_time);
extern void reset_span(Span * span);
extern const char *get_span_type(const Span * span);
extern const char *get_operation_name(const Span * span, const char *qbuffer, Size qbuffer_size);
extern void adjust_file_offset(Span * span, Size file_position);
extern bool traceid_zero(TraceId trace_id);
extern const char *span_type_to_str(SpanType span_type);


/* pg_tracing_sql_functions.c */
pgTracingStats get_empty_pg_tracing_stats(void);

/* pg_tracing_active_spans.c */
Span	   *pop_active_span(const TimestampTz *end_time);
Span	   *peek_active_span(void);
Span	   *push_active_span(MemoryContext context, const SpanContext * span_context,
							 SpanType span_type, HookPhase hook_phase);
Span	   *push_child_active_span(MemoryContext context, const SpanContext * span_context,
								   SpanType span_type);

void		cleanup_active_spans(void);

/* pg_tracing.c */
extern pgTracingSharedState * pg_tracing_shared_state;
extern pgTracingSpans * shared_spans;

extern int	nested_level;
extern void
			store_span(const Span * span);
extern int
			add_str_to_trace_buffer(const char *str, int str_len);
extern void
			cleanup_tracing(void);
extern void
			drop_all_spans_locked(void);
extern void
			pg_tracing_shmem_startup(void);
extern void reset_traceparent(Traceparent * traceparent);

/* pg_tracing_json.c */
extern void
			marshal_spans_to_json(JsonContext * json_ctx);
extern void
			build_json_context(JsonContext * json_ctx, const char *qbuffer, Size qbuffer_size, const pgTracingSpans * pgTracingSpans);

/* pg_tracing_otel.c */
extern void
			pg_tracing_start_worker(const char *endpoint, int naptime, int connect_timeout_ms);

#endif
