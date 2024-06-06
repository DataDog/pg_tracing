/*-------------------------------------------------------------------------
 *
 * pg_tracing_top_spans.c
 * 		functions managing top_spans.
 *
 * IDENTIFICATION
 *	  src/pg_tracing_top_spans.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_tracing.h"


/*
 * Create a new top_span for the current exec nested level
 */
Span *
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
 * Get the ongoing top span if it exists or create it
 */
Span *
get_or_allocate_top_span(pgTracingTraceContext * trace_context, bool in_parse_or_plan)
{
	Span	   *span;

	if (in_parse_or_plan && exec_nested_level == 0)

		/*
		 * The root post parse and plan, we want to use trace_context's
		 * root_span as the top span in per_level_buffers might still be
		 * ongoing.
		 */
		return &trace_context->root_span;

	if (per_level_buffers[exec_nested_level].top_spans->end == 0)
		/* No spans were created in this level, allocate a new one */
		span = allocate_new_top_span();
	else
		span = peek_top_span();

	if (exec_nested_level == 0)

		/*
		 * At root level and outside of parse/plan hook, we need to copy the
		 * root span content
		 */
		*span = trace_context->root_span;

	return span;
}

/*
 * Get the latest top span
 */
Span *
peek_top_span(void)
{
	pgTracingSpans *top_spans;

	top_spans = per_level_buffers[exec_nested_level].top_spans;
	Assert(top_spans->end > 0);
	return &top_spans->spans[top_spans->end - 1];
}

/*
 * Get the latest span for a specific level. The span must exists
 */
Span *
peek_nested_level_top_span(int nested_level)
{
	pgTracingSpans *top_spans;

	Assert(nested_level >= 0);
	top_spans = per_level_buffers[nested_level].top_spans;
	Assert(top_spans->end > 0);
	return &top_spans->spans[top_spans->end - 1];
}

/*
 * Drop the latest top span for the current nested level
 */
void
pop_top_span(void)
{
	pgTracingSpans *top_spans = per_level_buffers[exec_nested_level].top_spans;

	Assert(top_spans->end > 0);
	/* Reset span id of the discarded span since it could be reused */
	top_spans->spans[--top_spans->end].span_id = 0;
}
