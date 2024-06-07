/*-------------------------------------------------------------------------
 *
 * pg_tracing_active_spans.c
 * 		functions managing active spans.
 *
 * IDENTIFICATION
 *	  src/pg_tracing_active_spans.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_tracing.h"
#include "access/parallel.h"

static pgTracingSpans * active_spans = NULL;

/*
 * Add the worker name to the provided stringinfo
 */
static int
add_worker_name_to_trace_buffer(int parallel_worker_number)
{
	char	   *worker_name = psprintf("Worker %d", parallel_worker_number);

	return add_str_to_trace_buffer(worker_name, strlen(worker_name));
}

void
cleanup_active_spans(void)
{
	active_spans = NULL;
}

/*
 * Push a new active span to the active_spans stack
 */
Span *
allocate_new_active_span(void)
{
	Span	   *span;

	if (active_spans == NULL)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(pg_tracing_mem_ctx);
		active_spans = palloc0(sizeof(pgTracingSpans) + 10 * sizeof(Span));
		MemoryContextSwitchTo(oldcxt);
		active_spans->max = 10;
	}

	if (active_spans->end >= active_spans->max)
	{
		MemoryContext oldcxt;
		int			old_spans_max = active_spans->max;

		active_spans->max *= 2;
		oldcxt = MemoryContextSwitchTo(pg_tracing_mem_ctx);
		active_spans = repalloc0(active_spans,
								  sizeof(pgTracingSpans) + old_spans_max * sizeof(Span),
								  sizeof(pgTracingSpans) + old_spans_max * 2 * sizeof(Span));
		MemoryContextSwitchTo(oldcxt);
	}

	span = &active_spans->spans[active_spans->end++];
	span->nested_level = exec_nested_level;
	span->span_id = 0;
	return span;
}

/*
 * Get the latest top span
 */
Span *
peek_active_span(void)
{
	if (active_spans == NULL || active_spans->end == 0)
		return NULL;
	return &active_spans->spans[active_spans->end - 1];
}

/*
 * Drop the latest top span for the current nested level
 */
Span *
pop_active_span(void)
{
	if (active_spans == NULL || active_spans->end == 0)
		return NULL;

	Assert(active_spans->end > 0);
	return &active_spans->spans[--active_spans->end];
}

/*
 * Start a new top span if we've entered a new nested level or if the previous
 * span at the same level ended.
 */
static void
begin_active_span(pgTracingTraceContext * trace_context, Span * top_span,
				   CmdType commandType, const Query *query, const JumbleState *jstate,
				   const PlannedStmt *pstmt, const char *query_text, TimestampTz start_time,
				   bool export_parameters, Span * parent_span)
{
	int			query_len;
	const char *normalised_query;
	uint64		parent_id;
	int8		parent_planstate_index = -1;

	/* in case of a cached plan, query might be unavailable */
	if (query != NULL)
		per_level_buffers[exec_nested_level].query_id = query->queryId;
	else if (trace_context->query_id > 0)
		per_level_buffers[exec_nested_level].query_id = trace_context->query_id;

	if (exec_nested_level == 0)
		/* Root top span, use the parent id from the trace context */
		parent_id = trace_context->traceparent.parent_id;
	else
	{
		TracedPlanstate *parent_traced_planstate = NULL;

		Assert(parent_span != NULL);

		/*
		 * We're in a nested level, check if we have a parent planstate and
		 * use it as a parent span
		 */
		parent_planstate_index = get_parent_traced_planstate_index(exec_nested_level);
		if (parent_planstate_index > -1)
			parent_traced_planstate = get_traced_planstate_from_index(parent_planstate_index);

		/*
		 * Both planstate and previous top span can be the parent for the new
		 * top span, we use the most recent as a parent
		 */
		if (parent_traced_planstate != NULL && parent_traced_planstate->node_start >= parent_span->start)
			parent_id = parent_traced_planstate->span_id;
		else
			parent_id = parent_span->span_id;
	}

	begin_span(trace_context->traceparent.trace_id, top_span,
			   command_type_to_span_type(commandType),
			   NULL, parent_id,
			   per_level_buffers[exec_nested_level].query_id, &start_time);
	/* Keep track of the parent planstate index */
	top_span->parent_planstate_index = parent_planstate_index;

	if (IsParallelWorker())
	{
		/*
		 * In a parallel worker, we use the worker name as the span's
		 * operation
		 */
		top_span->operation_name_offset = add_worker_name_to_trace_buffer(ParallelWorkerNumber);
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
		if (export_parameters)
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
	if (query_len > 0)
		top_span->operation_name_offset = add_str_to_trace_buffer(normalised_query,
																  query_len);
}

/*
 * End the latest top span for the current nested level.
 * If pop_span is true, store the span in the current_trace_spans and remove it
 * from the per level buffers.
 */
void
end_latest_active_span(const TimestampTz *end_time)
{
	Span	   *top_span;

	/* Check if the level was allocated */
	if (exec_nested_level > max_nested_level)
		return;

	top_span = pop_active_span();
	end_span(top_span, end_time);
	store_span(top_span);
}

/*
 * Initialise buffers if we are in a new nested level and start associated top span.
 * If the top span already exists for the current nested level, this has no effect.
 *
 * This needs to be called every time a top span could be started: post parse,
 * planner, executor start and process utility
 */
uint64
initialize_active_span(pgTracingTraceContext * trace_context, CmdType commandType,
						Query *query, JumbleState *jstate, const PlannedStmt *pstmt,
						const char *query_text, TimestampTz start_time,
						bool in_parse_or_plan, bool export_parameters)
{
	Span	   *span;
	Span	   *parent_span;

	/* Get latest active span to be used as parent */
	parent_span = peek_active_span();

	if (in_parse_or_plan && exec_nested_level == 0)

		/*
		 * The root post parse and plan, we want to use trace_context's
		 * root_span as the top span in per_level_buffers might still be
		 * active.
		 */
		span = &trace_context->root_span;
	else
	{
		if (active_spans == NULL || active_spans->end == 0)
		{
			/* first creation of active spans */
			span = allocate_new_active_span();
			Assert(exec_nested_level == 0);
			/* we need to copy the root span content */
			*span = trace_context->root_span;
			return span->span_id;
		}
		span = peek_active_span();
		if (span->nested_level < exec_nested_level)
			/* Span belongs to a previous level, create a new one */
			span = allocate_new_active_span();
	}

	/* If the span is still active, use it as it is */
	if (span->nested_level == exec_nested_level
		&& span->span_id > 0)
		return span->span_id;

	/* This is a new top span, start it */
	begin_active_span(trace_context, span, commandType, query, jstate, pstmt,
					   query_text, start_time, export_parameters, parent_span);
	return span->span_id;
}