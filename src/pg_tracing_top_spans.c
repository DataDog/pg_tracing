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
 * Get the latest span for a specific level. The span must exists
 */
Span *
get_latest_top_span(int nested_level)
{
	pgTracingSpans *top_spans;

	Assert(nested_level >= 0);
	top_spans = per_level_buffers[nested_level].top_spans;
	Assert(top_spans->end > 0);
	return &top_spans->spans[top_spans->end - 1];
}

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
