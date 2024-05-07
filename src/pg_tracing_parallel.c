/*-------------------------------------------------------------------------
 *
 * pg_tracing_parallel.c
 * 		Store, retrieve and remove trace context for parallel workers.
 *
 * IDENTIFICATION
 *	  src/pg_tracing_parallel.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/shmem.h"
#include "storage/spin.h"
#include "pg_tracing.h"

/* Shared buffer storing trace context for parallel workers. */
static pgTracingParallelWorkers * pg_tracing_parallel = NULL;

/* Index of the parallel worker context shared buffer if any */
static int	parallel_context_index = -1;

/*
 * Allocate share memory for propagation of trace context to parallel workers
 */
void
pg_tracing_shmem_parallel_startup(void)
{
	bool		found_parallel;

	/* We won't have more than max_parallel_workers workers */
	pg_tracing_parallel = ShmemInitStruct("PgTracing Parallel Workers Context",
										  sizeof(pgTracingParallelWorkers) + max_parallel_workers * sizeof(pgTracingParallelContext),
										  &found_parallel);
	if (!found_parallel)
	{
		SpinLockInit(&pg_tracing_parallel->mutex);
		for (int i = 0; i < max_parallel_workers; i++)
			pg_tracing_parallel->trace_contexts[i].leader_backend_id = INVALID_PROC_NUMBER;
	}
}

/*
 * Push trace context to the shared parallel worker buffer
 */
void
add_parallel_context(const struct pgTracingTraceContext *trace_context,
					 uint64 parent_id, uint64 query_id)
{
	pgTracingParallelContext *ctx = NULL;

	Assert(parallel_context_index == -1);
	SpinLockAcquire(&pg_tracing_parallel->mutex);
	for (int i = 0; i < max_parallel_workers; i++)
	{
		ctx = pg_tracing_parallel->trace_contexts + i;
		Assert(ctx->leader_backend_id != MyProcNumber);
		if (ctx->leader_backend_id != INVALID_PROC_NUMBER)
			continue;
		/* Slot is available */
		parallel_context_index = i;
		ctx->leader_backend_id = MyProcNumber;
		/* We can do the rest outside the lock */
		break;
	}
	SpinLockRelease(&pg_tracing_parallel->mutex);

	if (parallel_context_index > -1 && ctx != NULL)
	{
		ctx->trace_context = *trace_context;
		/* We don't need to propagate root span index to parallel workers */
		ctx->trace_context.root_span.span_id = 0;
		ctx->trace_context.traceparent.parent_id = parent_id;
	}
}

/*
 * Remove parallel context for the current leader from the shared memory.
 */
void
remove_parallel_context(void)
{
	if (parallel_context_index < 0)
		/* No tracing of parallel workers */
		return;

	SpinLockAcquire(&pg_tracing_parallel->mutex);
	pg_tracing_parallel->trace_contexts[parallel_context_index].leader_backend_id = INVALID_PROC_NUMBER;
	SpinLockRelease(&pg_tracing_parallel->mutex);
	parallel_context_index = -1;
}

/*
 * If we're inside a parallel worker, check if the trace context is stored in shared memory.
 * If a trace context exists, it means that the query is sampled and worker tracing is enabled.
 */
void
fetch_parallel_context(pgTracingTraceContext * trace_context)
{
	SpinLockAcquire(&pg_tracing_parallel->mutex);
	for (int i = 0; i < max_parallel_workers; i++)
	{
		if (pg_tracing_parallel->trace_contexts[i].leader_backend_id != ParallelLeaderProcNumber)
			continue;
		/* Found a matching a trace context, fetch it */
		*trace_context = pg_tracing_parallel->trace_contexts[i].trace_context;
	}
	SpinLockRelease(&pg_tracing_parallel->mutex);
}
