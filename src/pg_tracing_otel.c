/*-------------------------------------------------------------------------
 *
 * pg_tracing_otel.c
 * 		pg_tracing otel export functions.
 *
 * IDENTIFICATION
 *	  src/pg_tracing_otel.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_tracing.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "utils/memutils.h"
#include "storage/latch.h"
#include "storage/procsignal.h"
#include <curl/curl.h>

/* Background worker entry point */
PGDLLEXPORT void pg_tracing_otel_exporter(Datum main_arg);

typedef struct OtelContext
{
	CURL	   *curl;			/* Curl handle */
	struct curl_slist *headers; /* list of http headers common to all requests */
	pgTracingSpans *spans;		/* A copy of spans to send */
	char	   *spans_str;		/* A copy of span text */

	const char *endpoint;		/* Target otel collector */
	int			naptime;		/* Duration between upload ot spans to the
								 * otel collector */
	int			connect_timeout_ms; /* Connection timeout in ms */
}			OtelContext;

/* State and configuration of the otel exporter */
static OtelContext otel_context;

/* Dedicated memory contexts for otel exporter background worker. */
static MemoryContext otel_exporter_mem_ctx;

/* Memory context used for json marshalling */
static MemoryContext marshal_mem_ctx;

/* Memory context used for libcurl */
static MemoryContext curl_mem_ctx;

/* Curl memory callback functions */

static void *
pg_tracing_curl_malloc_callback(size_t size)
{
	if (size)
		return MemoryContextAlloc(curl_mem_ctx, size);
	return NULL;
}

static void
pg_tracing_curl_free_callback(void *ptr)
{
	if (ptr)
		pfree(ptr);
}

static void *
pg_tracing_curl_realloc_callback(void *ptr, size_t size)
{
	if (ptr && size)
		return repalloc(ptr, size);
	if (size)
		return MemoryContextAlloc(curl_mem_ctx, size);
	return ptr;
}

static char *
pg_tracing_curl_strdup_callback(const char *str)
{
	return MemoryContextStrdup(curl_mem_ctx, str);
}

static void *
pg_tracing_curl_calloc_callback(size_t nmemb, size_t size)
{
	return MemoryContextAllocZero(curl_mem_ctx, nmemb * size);
}

/*
 * Send json to configured otel http endpoint
 */
static CURLcode
send_json_trace(OtelContext * octx, const char *json_span)
{
	CURLcode	res;

	if (octx->curl == NULL)
	{
		/*
		 * Keep a single handle and don't clean it to keep the connection
		 * opened
		 */
		octx->curl = curl_easy_init();
		if (octx->curl == NULL)
		{
			elog(ERROR, "Couldn't initialize curl handle");
			return CURLE_FAILED_INIT;
		}
		curl_easy_setopt(octx->curl, CURLOPT_HTTPHEADER, octx->headers);
		curl_easy_setopt(octx->curl, CURLOPT_URL, octx->endpoint);
		curl_easy_setopt(octx->curl, CURLOPT_CONNECTTIMEOUT_MS, octx->connect_timeout_ms);
	}
	curl_easy_setopt(octx->curl, CURLOPT_POSTFIELDS, json_span);
	curl_easy_setopt(octx->curl, CURLOPT_POSTFIELDSIZE, (long) strlen(json_span));

	res = curl_easy_perform(octx->curl);
	return res;
}

static void
copy_spans_to_context(OtelContext * octx)
{
	Size		span_size = sizeof(pgTracingSpans) + shared_spans->end * sizeof(Span);

	Assert(octx->spans == NULL);
	Assert(octx->spans_str == NULL);

	/* Copy spans to send */
	octx->spans = palloc(span_size);
	memcpy(octx->spans, shared_spans, span_size);
	/* Copy shared str */
	octx->spans_str = palloc(pg_tracing_shared_state->extent);
	memcpy(octx->spans_str, shared_str, pg_tracing_shared_state->extent);
}

static void
send_json_to_otel_collector(OtelContext * octx, JsonContext * json_ctx)
{
	CURLcode	ret;

	elog(INFO, "Sending %d spans to %s", json_ctx->num_spans, octx->endpoint);
	ret = send_json_trace(octx, json_ctx->str->data);
	if (ret == CURLE_OK)
	{
		pg_tracing_shared_state->stats.otel_sent_spans += json_ctx->num_spans;
		/* Send was successful, free the spans and spans_str copy */
		MemoryContextReset(marshal_mem_ctx);
		/* and reset our json_ctx stringinfo */
		json_ctx->str = NULL;
	}
	else
	{
		ereport(WARNING, errmsg("curl_easy_perform() failed: %s\n",
								curl_easy_strerror(ret)));

		/*
		 * On a failure, we keep the json payload and will retry to send it on
		 * a next attempt
		 */
		pg_tracing_shared_state->stats.otel_failures++;
	}
}

/*
 * Consume and send spans to the otel collector
 */
static void
send_spans_to_otel_collector(OtelContext * octx, JsonContext * json_ctx)
{
	int			num_spans = 0;

	if (json_ctx->str && json_ctx->str->len > 0)
	{
		/*
		 * We have a previous json payload that we failed to send, try to send
		 * it
		 */
		send_json_to_otel_collector(octx, json_ctx);
		return;
	}

	LWLockAcquire(pg_tracing_shared_state->lock, LW_EXCLUSIVE);
	/* Check if we have spans to send */
	if (shared_spans->end == 0)
	{
		LWLockRelease(pg_tracing_shared_state->lock);
		return;
	}
	num_spans = shared_spans->end;

	/*
	 * We copy the spans and spans_str in the otel context to release the lock
	 * as soon as possible
	 */
	copy_spans_to_context(octx);

	/* Copy is done, drop all spans */
	drop_all_spans_locked();
	/* and free the lock */
	LWLockRelease(pg_tracing_shared_state->lock);

	/* Do marshalling within marshal memory context */
	MemoryContextSwitchTo(marshal_mem_ctx);

	/* Build the json context */
	build_json_context(json_ctx, octx->spans, octx->spans_str, num_spans);
	/* Do the json marshalling */
	marshal_spans_to_json(json_ctx);

	MemoryContextSwitchTo(otel_exporter_mem_ctx);

	/* Marshalling is done, we can release our spans and spans_str copy */
	pfree(octx->spans);
	pfree(octx->spans_str);
	octx->spans = NULL;
	octx->spans_str = NULL;

	if (json_ctx->str->len > 0)
		send_json_to_otel_collector(octx, json_ctx);
}

/*
 * Register otel exporter background worker
 */
void
pg_tracing_start_worker(const char *endpoint, int naptime, int connect_timeout_ms)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	strcpy(worker.bgw_library_name, "pg_tracing");
	strcpy(worker.bgw_function_name, "pg_tracing_otel_exporter");
	strcpy(worker.bgw_name, "pg_tracing otel exporter");
	strcpy(worker.bgw_type, "pg_tracing otel exporter");

	/* Initialize the otel context struct */
	otel_context.headers = NULL;
	otel_context.curl = NULL;
	otel_context.endpoint = endpoint;
	otel_context.naptime = naptime;
	otel_context.connect_timeout_ms = connect_timeout_ms;

	if (process_shared_preload_libraries_in_progress)
	{
		RegisterBackgroundWorker(&worker);
		return;
	}

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register background process"),
				 errhint("You may need to increase max_worker_processes.")));

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));
}

/*
 * Entry point for otel exporter background worker
 *
 * Initialize all memory contexts and start the main loop that
 * will send the spans to the configured otel collector
 */
void
pg_tracing_otel_exporter(Datum main_arg)
{
	JsonContext json_ctx;

	json_ctx.str = NULL;

	/* Establish signal handlers; once that's done, unblock signals. */
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	BackgroundWorkerUnblockSignals();

	/* Go through pg_tracing shmem startup to attach the shared_spans global */
	pg_tracing_shmem_startup();

	/* Initialize otel exporter memory context */
	otel_exporter_mem_ctx = AllocSetContextCreate(TopMemoryContext,
												  "pg_tracing otel exporter",
												  ALLOCSET_DEFAULT_SIZES);
	/* And switch to the created context */
	MemoryContextSwitchTo(otel_exporter_mem_ctx);

	/* Create marshalling and curl context as children contexts */
	marshal_mem_ctx = AllocSetContextCreate(otel_exporter_mem_ctx,
											"json marshalling",
											ALLOCSET_DEFAULT_SIZES);
	curl_mem_ctx = AllocSetContextCreate(otel_exporter_mem_ctx,
										 "libcurl",
										 ALLOCSET_DEFAULT_SIZES);

	/* Initialize libcurl */
	if (curl_global_init_mem(CURL_GLOBAL_ALL, pg_tracing_curl_malloc_callback,
							 pg_tracing_curl_free_callback,
							 pg_tracing_curl_realloc_callback,
							 pg_tracing_curl_strdup_callback,
							 pg_tracing_curl_calloc_callback))
		ereport(ERROR, (
						errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("curl_global_init_mem")));


	/*
	 * Create the content type header only once since it will always be the
	 * same
	 */
	otel_context.headers = curl_slist_append(otel_context.headers, "Content-Type: application/json");

	while (!ShutdownRequestPending)
	{
		int			rc;

		/* Clean the latch and wait for the next event */
		ResetLatch(MyLatch);
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   otel_context.naptime,
					   PG_WAIT_EXTENSION);

		/* Send spans if we have any */
		if (rc & WL_TIMEOUT)
			send_spans_to_otel_collector(&otel_context, &json_ctx);
	}

	/* Curl cleanup */
	curl_slist_free_all(otel_context.headers);
	otel_context.headers = NULL;
	if (otel_context.curl)
	{
		curl_easy_cleanup(otel_context.curl);
		otel_context.curl = NULL;
	}
	curl_global_cleanup();
}
