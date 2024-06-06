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
#include "storage/backendid.h"
#include "storage/lwlock.h"
#include "version_compat.h"

/* Location of external text file */
#define PG_TRACING_TEXT_FILE	PG_STAT_TMP_DIR "/pg_tracing.stat"

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
 * SpanType: Type of the span
 */
typedef enum SpanType
{
	SPAN_PLANNER,				/* Wraps planner execution in planner hook */
	SPAN_FUNCTION,				/* Wraps function in fmgr hook */
	SPAN_PROCESS_UTILITY,		/* Wraps ProcessUtility execution */

	SPAN_EXECUTOR_RUN,			/* Wraps Executor run hook */
	SPAN_EXECUTOR_FINISH,		/* Wraps Executor finish hook */
	SPAN_COMMIT,				/* Wraps time between pre-commit and commit */

	/* Represents a node execution, generated from planstate */
	SPAN_NODE,
	SPAN_NODE_INIT_PLAN,
	SPAN_NODE_SUBPLAN,

	/* Top Span types. They are created from the query cmdType */
	SPAN_TOP_SELECT,
	SPAN_TOP_INSERT,
	SPAN_TOP_UPDATE,
	SPAN_TOP_DELETE,
	SPAN_TOP_MERGE,
	SPAN_TOP_UTILITY,
	SPAN_TOP_NOTHING,
	SPAN_TOP_UNKNOWN,
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
	bool		ended;			/* Track if the span was already ended.
								 * Internal usage only */
	int8		parent_planstate_index; /* Index to the parent planstate of
										 * this span. Internal usage only */
	int			be_pid;			/* Pid of the backend process */
	Oid			user_id;		/* User ID when the span was created */
	Oid			database_id;	/* Database ID where the span was created */

	uint8		subxact_count;	/* Active count of backend's subtransaction */

	/*
	 * We store variable size metadata in an external file. Those represent
	 * the position of NULL terminated strings in the file. Set to -1 if
	 * unused.
	 */
	Size		node_type_offset;	/* name of the node type */
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
typedef struct pgTracingTraceparent
{
	TraceId		trace_id;		/* Id of the trace */
	uint64		parent_id;		/* Span id of the parent */
	int			sampled;		/* Is current statement sampled? */
}			pgTracingTraceparent;

/*
 * Trace context for an ongoing trace
 */
typedef struct pgTracingTraceContext
{
	pgTracingTraceparent traceparent;
	uint64		query_id;		/* Query id of the current statement */
	Span		root_span;		/* Top span for exec_nested_level 0 */
}			pgTracingTraceContext;

/*
 * A trace context for a specific parallel context
 */
typedef struct pgTracingParallelContext
{
	ProcNumber	leader_backend_id;	/* Backend id of the leader, set to
									 * InvalidBackendId if unused */
	pgTracingTraceContext trace_context;
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

/* pg_tracing_explain.c */
extern const char *plan_to_node_type(const Plan *plan);
extern const char *plan_to_operation(const planstateTraceContext * planstateTraceContext, const PlanState *planstate, const char *spanName);
extern const char *plan_to_deparse_info(const planstateTraceContext * planstateTraceContext, const PlanState *planstate);

/* pg_tracing_parallel.c */
extern void pg_tracing_shmem_parallel_startup(void);
extern void add_parallel_context(const struct pgTracingTraceContext *trace_context,
								 uint64 parent_id, uint64 query_id);
extern void remove_parallel_context(void);
extern void fetch_parallel_context(pgTracingTraceContext * trace_context);

/* pg_tracing_planstate.c */

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

extern Span
create_span_node(PlanState *planstate, const planstateTraceContext * planstateTraceContext,
				 uint64 *span_id, uint64 parent_id, uint64 queryId, SpanType span_type,
				 char *subplan_name, TimestampTz span_start, TimestampTz span_end);
extern TimestampTz
			generate_span_from_planstate(PlanState *planstate, planstateTraceContext * planstateTraceContext,
										 uint64 parent_id, uint64 query_id,
										 TimestampTz parent_start, TimestampTz root_end, TimestampTz *latest_end);
extern void
			setup_ExecProcNode_override(QueryDesc *queryDesc);
extern void
			cleanup_planstarts(void);
extern TracedPlanstate *
get_traced_planstate_from_index(int index);
extern int
			get_parent_traced_planstate_index(int nested_level);
extern void
			drop_traced_planstate(int exec_nested_level);
extern TimestampTz
			get_span_end_from_planstate(PlanState *planstate, TimestampTz plan_start, TimestampTz root_end);

/* pg_tracing_query_process.c */
extern const char *normalise_query_parameters(const JumbleState *jstate, const char *query,
											  int query_loc, int *query_len_p, char **param_str,
											  int *param_len);
extern void extract_trace_context_from_query(pgTracingTraceContext * trace_context, const char *query);
extern void parse_trace_context(pgTracingTraceContext * trace_context, const char *trace_context_str, int trace_context_len);
extern const char *normalise_query(const char *query, int query_loc, int *query_len_p);
extern bool text_store_file(pgTracingSharedState * pg_tracing, const char *query,
							int query_len, Size *query_offset);
extern const char *qtext_load_file(Size *buffer_size);
extern const char *qtext_load_file(Size *buffer_size);

/* pg_tracing_span.c */
extern void begin_span(TraceId trace_id, Span * span,
					   SpanType type, uint64 *span_id, uint64 parent_id, uint64 query_id, const TimestampTz *start_span);
extern void end_span(Span * span, const TimestampTz *end_time);
extern SpanType command_type_to_span_type(CmdType cmd_type);
extern const char *get_span_type(const Span * span, const char *qbuffer, Size qbuffer_size);
extern const char *get_operation_name(const Span * span, const char *qbuffer, Size qbuffer_size);
extern void adjust_file_offset(Span * span, Size file_position);
extern bool traceid_zero(TraceId trace_id);


/* pg_tracing.c */
extern MemoryContext pg_tracing_mem_ctx;
extern int	exec_nested_level;
extern void
			store_span(const Span * span);
extern int
			add_str_to_trace_buffer(const char *str, int str_len);

#endif
