/*-------------------------------------------------------------------------
 *
 * pg_tracing_span.c
 * 		pg_tracing span functions.
 *
 * IDENTIFICATION
 *	  src/pg_tracing_span.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/pg_prng.h"
#include "storage/proc.h"
#include "pg_tracing.h"

/*
 * Initialize span fields
 */
void
begin_span(TraceId trace_id, Span * span, SpanType type,
		   const uint64 *span_id, uint64 parent_id, uint64 query_id,
		   TimestampTz start_span)
{
	span->start = start_span;
	span->trace_id = trace_id;
	span->type = type;

	/*
	 * If parent id is unset, it means that there's no propagated trace
	 * informations from the caller. In this case, this is the top span,
	 * span_id == parent_id and we reuse the generated trace id for the span
	 * id.
	 */
	if (parent_id == 0)
		span->parent_id = trace_id.traceid_right;
	else
		span->parent_id = parent_id;
	if (span_id != NULL)
		span->span_id = *span_id;
	else
		span->span_id = pg_prng_uint64(&pg_global_prng_state);

	span->node_type_offset = -1;
	span->operation_name_offset = -1;
	span->parameter_offset = -1;
	span->deparse_info_offset = -1;
	span->sql_error_code = 0;
	span->startup = 0;
	span->be_pid = MyProcPid;
	span->database_id = MyDatabaseId;
	span->user_id = GetUserId();
	span->subxact_count = MyProc->subxidStatus.count;
	span->query_id = query_id;
	memset(&span->node_counters, 0, sizeof(NodeCounters));
	memset(&span->plan_counters, 0, sizeof(PlanCounters));

	/*
	 * Store the starting buffer for planner and process utility spans
	 */
	if (type == SPAN_PLANNER || span->type == SPAN_PROCESS_UTILITY)
	{
		span->node_counters.buffer_usage = pgBufferUsage;
		span->node_counters.wal_usage = pgWalUsage;
	}
}

/*
 * Set span duration and accumulated buffers.
 * end_span_input is optional, if NULL is passed, we use
 * the current time
 */
void
end_span(Span * span, const TimestampTz *end_time_input)
{
	BufferUsage buffer_usage;
	WalUsage	wal_usage;

	Assert(!traceid_zero(span->trace_id));

	/* Set span duration with the end time before subtracting the start */
	if (end_time_input == NULL)
		span->end = GetCurrentTimestamp();
	else
		span->end = *end_time_input;


	if (span->type == SPAN_PLANNER || span->type == SPAN_PROCESS_UTILITY)
	{
		/* calculate differences of buffer counters. */
		memset(&buffer_usage, 0, sizeof(BufferUsage));
		BufferUsageAccumDiff(&buffer_usage, &pgBufferUsage,
							 &span->node_counters.buffer_usage);
		span->node_counters.buffer_usage = buffer_usage;

		/* calculate differences of WAL counters. */
		memset(&wal_usage, 0, sizeof(wal_usage));
		WalUsageAccumDiff(&wal_usage, &pgWalUsage,
						  &span->node_counters.wal_usage);
		span->node_counters.wal_usage = wal_usage;
	}
}

/*
 * Reset span
 */
void
reset_span(Span * span)
{
	span->span_id = 0;
}

/*
 * Get the type of a span.
 * If it is a node span, the name may be pulled from the stat file.
 */
const char *
get_span_type(const Span * span, const char *qbuffer, Size qbuffer_size)
{
	if (span->node_type_offset != -1 && qbuffer_size > 0 &&
		span->node_type_offset <= qbuffer_size)
	{
		StringInfo	node_name = makeStringInfo();

		appendStringInfoString(node_name, qbuffer + span->node_type_offset);
		return node_name->data;
	}

	switch (span->type)
	{
		case SPAN_COMMIT:
			return "Commit";
		case SPAN_PLANNER:
			return "Planner";
		case SPAN_FUNCTION:
			return "Function";
		case SPAN_PROCESS_UTILITY:
			return "ProcessUtility";
		case SPAN_EXECUTOR_RUN:
			return "Executor";
		case SPAN_EXECUTOR_FINISH:
			return "Executor";

		case SPAN_TOP_SELECT:
			return "Select query";
		case SPAN_TOP_INSERT:
			return "Insert query";
		case SPAN_TOP_UPDATE:
			return "Update query";
		case SPAN_TOP_DELETE:
			return "Delete query";
		case SPAN_TOP_MERGE:
			return "Merge query";
		case SPAN_TOP_UTILITY:
			return "Utility query";
		case SPAN_TOP_NOTHING:
			return "Nothing query";
		case SPAN_TOP_UNKNOWN:
			return "Unknown query";

		case SPAN_NODE_INIT_PLAN:
			return "InitPlan";
		case SPAN_NODE_SUBPLAN:
			return "SubPlan";
		case SPAN_NODE:
			return "Node";
	}
	return "???";
}

/*
 * Get the operation of a span.
 * For node span, the name may be pulled from the stat file.
 */
const char *
get_operation_name(const Span * span, const char *qbuffer, Size qbuffer_size)
{
	if (span->operation_name_offset != -1 && qbuffer_size > 0
		&& span->operation_name_offset <= qbuffer_size)
		return qbuffer + span->operation_name_offset;

	switch (span->type)
	{
		case SPAN_COMMIT:
			return "Commit";
		case SPAN_PLANNER:
			return "Planner";
		case SPAN_FUNCTION:
			return "Function";
		case SPAN_PROCESS_UTILITY:
			return "ProcessUtility";
		case SPAN_EXECUTOR_RUN:
			return "ExecutorRun";
		case SPAN_EXECUTOR_FINISH:
			return "ExecutorFinish";
		case SPAN_TOP_SELECT:
		case SPAN_TOP_INSERT:
		case SPAN_TOP_UPDATE:
		case SPAN_TOP_DELETE:
		case SPAN_TOP_MERGE:
		case SPAN_TOP_UTILITY:
		case SPAN_TOP_NOTHING:
		case SPAN_TOP_UNKNOWN:
			return "Top";
		case SPAN_NODE_INIT_PLAN:
		case SPAN_NODE_SUBPLAN:
		case SPAN_NODE:
			return "Node";
	}
	return "Unknown type";
}

/*
 * Adjust span's offsets with the provided file offset
 */
void
adjust_file_offset(Span * span, Size file_position)
{
	if (span->node_type_offset != -1)
		span->node_type_offset += file_position;
	if (span->operation_name_offset != -1)
		span->operation_name_offset += file_position;
	if (span->parameter_offset != -1)
		span->parameter_offset += file_position;
	if (span->deparse_info_offset != -1)
		span->deparse_info_offset += file_position;
}

/*
* Returns true if TraceId is zero
*/
bool
traceid_zero(TraceId trace_id)
{
	return trace_id.traceid_left == 0 && trace_id.traceid_right == 0;
}
