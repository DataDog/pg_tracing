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

	span->parallel_aware = false;
	span->async_capable = false;
	span->worker_id = -1;
	span->operation_name_offset = -1;
	span->num_parameters = 0;
	span->num_truncated_parameters = 0;
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

	Assert(span->end >= span->start);

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
 * Convert span_type to string
 */
const char *
span_type_to_str(SpanType span_type)
{
	switch (span_type)
	{
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
		case SPAN_TRANSACTION_COMMIT:
			return "TransactionCommit";
		case SPAN_TRANSACTION_BLOCK:
			return "TransactionBlock";

		case SPAN_NODE:
			return "Node";
		case SPAN_NODE_INIT_PLAN:
			return "InitPlan";
		case SPAN_NODE_SUBPLAN:
			return "SubPlan";
		case SPAN_NODE_RESULT:
			return "Result";
		case SPAN_NODE_PROJECT_SET:
			return "ProjectSet";
		case SPAN_NODE_INSERT:
			return "Insert";
		case SPAN_NODE_UPDATE:
			return "Update";
		case SPAN_NODE_DELETE:
			return "Delete";
		case SPAN_NODE_MERGE:
			return "Merge";
		case SPAN_NODE_APPEND:
			return "Append";
		case SPAN_NODE_MERGE_APPEND:
			return "MergeAppend";
		case SPAN_NODE_RECURSIVE_UNION:
			return "RecursiveUnion";
		case SPAN_NODE_BITMAP_AND:
			return "BitmapAnd";
		case SPAN_NODE_BITMAP_OR:
			return "BitmapOr";
		case SPAN_NODE_NESTLOOP:
			return "NestedLoop";
		case SPAN_NODE_MERGE_JOIN:
			return "Merge";		/* Join is added later by the join type */
		case SPAN_NODE_HASH_JOIN:
			return "Hash";		/* Join is added later by the join type */
		case SPAN_NODE_SEQ_SCAN:
			return "SeqScan";
		case SPAN_NODE_SAMPLE_SCAN:
			return "SampleScan";
		case SPAN_NODE_GATHER:
			return "Gather";
		case SPAN_NODE_GATHER_MERGE:
			return "GatherMerge";
		case SPAN_NODE_INDEX_SCAN:
			return "IndexScan";
		case SPAN_NODE_INDEX_ONLY_SCAN:
			return "IndexOnlyScan";
		case SPAN_NODE_BITMAP_INDEX_SCAN:
			return "BitmapIndexScan";
		case SPAN_NODE_BITMAP_HEAP_SCAN:
			return "BitmapHeapScan";
		case SPAN_NODE_TID_SCAN:
			return "TidScan";
		case SPAN_NODE_TID_RANGE_SCAN:
			return "TidRangeScan";
		case SPAN_NODE_SUBQUERY_SCAN:
			return "SubqueryScan";
		case SPAN_NODE_FUNCTION_SCAN:
			return "FunctionScan";
		case SPAN_NODE_TABLEFUNC_SCAN:
			return "TablefuncScan";
		case SPAN_NODE_VALUES_SCAN:
			return "ValuesScan";
		case SPAN_NODE_CTE_SCAN:
			return "CTEScan";
		case SPAN_NODE_NAMED_TUPLE_STORE_SCAN:
			return "NamedTupleStoreScan";
		case SPAN_NODE_WORKTABLE_SCAN:
			return "WorktableScan";
		case SPAN_NODE_FOREIGN_SCAN:
			return "ForeignScan";
		case SPAN_NODE_FOREIGN_INSERT:
			return "ForeignInsert";
		case SPAN_NODE_FOREIGN_UPDATE:
			return "ForeignUpdate";
		case SPAN_NODE_FOREIGN_DELETE:
			return "ForeignDelete";
		case SPAN_NODE_CUSTOM_SCAN:
			return "CustomScan";
		case SPAN_NODE_MATERIALIZE:
			return "Materialize";
		case SPAN_NODE_MEMOIZE:
			return "Memoize";
		case SPAN_NODE_SORT:
			return "Sort";
		case SPAN_NODE_INCREMENTAL_SORT:
			return "IncrementalSort";
		case SPAN_NODE_GROUP:
			return "Group";
		case SPAN_NODE_AGGREGATE:
			return "Aggregate";
		case SPAN_NODE_GROUP_AGGREGATE:
			return "GroupAggregate";
		case SPAN_NODE_HASH_AGGREGATE:
			return "HashAggregate";
		case SPAN_NODE_MIXED_AGGREGATE:
			return "MixedAggregate";
		case SPAN_NODE_WINDOW_AGG:
			return "WindowAgg";
		case SPAN_NODE_UNIQUE:
			return "Unique";
		case SPAN_NODE_SETOP:
			return "Setop";
		case SPAN_NODE_SETOP_HASHED:
			return "SetopHashed";
		case SPAN_NODE_LOCK_ROWS:
			return "LockRows";
		case SPAN_NODE_LIMIT:
			return "Limit";
		case SPAN_NODE_HASH:
			return "Hash";
		case SPAN_NODE_UNKNOWN:
			return "UnknownNode";

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
		case NUM_SPAN_TYPE:
			return "Unknown type";
	}
	return "Unknown";
}

static bool
is_span_top(SpanType span_type)
{
	return span_type >= SPAN_TOP_SELECT
		&& span_type <= SPAN_TOP_UNKNOWN;
}

/*
 * Get the operation of a span.
 * For node span, the name may be pulled from the stat file.
 */
const char *
get_operation_name(const Span * span)
{
	const char *span_type_str;
	const char *operation_str = NULL;

	/* Use worker id when available */
	if (span->worker_id >= 0)
	{
		Assert(span->operation_name_offset == -1);
		return psprintf("Worker %d", span->worker_id);
	}

	span_type_str = span_type_to_str(span->type);
	/* TODO: Check for maximum offset */
	if (span->operation_name_offset != -1)
		operation_str = shared_str + span->operation_name_offset;
	else
		return span_type_str;

	/* Top span, just grab the query string */
	if (is_span_top(span->type))
	{
		if (operation_str)
			return operation_str;
		/* Fallback to span type */
		return span_type_str;
	}

	if (span->type == SPAN_NODE_INIT_PLAN || span->type == SPAN_NODE_SUBPLAN)
		return operation_str;

	/* Node span, prepend with the span type */
	if (operation_str)
		return psprintf("%s %s", span_type_str, operation_str);
	return span_type_str;
}

/*
 * Adjust span's offsets with the provided file offset
 */
void
adjust_file_offset(Span * span, Size file_position)
{
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

/*
* Returns true if trace ids are equals
*/
bool
traceid_equal(TraceId trace_id_1, TraceId trace_id_2)
{
	return trace_id_1.traceid_left == trace_id_2.traceid_left &&
		trace_id_1.traceid_right == trace_id_2.traceid_right;
}
