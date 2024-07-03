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
#include "utils/timestamp.h"
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
	const char *endpoint;		/* Target otel collector */
	int			naptime;		/* Duration between upload ot spans to the
								 * otel collector */
	int			connect_timeout_ms; /* Connection timeout in ms */
}			OtelContext;

/* State and configuration of the otel exporter */
static OtelContext otelContext;

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

/*
 * Consume and send spans to the otel collector
 */
static void
send_spans_to_otel_collector(OtelContext * octx)
{
	int			num_spans = 0;
	char	   *qbuffer;
	Size		qbuffer_size = 0;
	JsonContext json_ctx;

	/*
	 * Do a quick check with a shared lock to see if there are any spans to
	 * send
	 */
	LWLockAcquire(pg_tracing_shared_state->lock, LW_SHARED);
	if (shared_spans->end == 0)
	{
		LWLockRelease(pg_tracing_shared_state->lock);
		return;
	}
	LWLockRelease(pg_tracing_shared_state->lock);

	LWLockAcquire(pg_tracing_shared_state->lock, LW_EXCLUSIVE);
	/* Recheck for spans to send */
	if (shared_spans->end == 0)
	{
		LWLockRelease(pg_tracing_shared_state->lock);
		return;
	}
	num_spans = shared_spans->end;

	qbuffer = qtext_load_file(&qbuffer_size);
	if (qbuffer == NULL)
	{
		LWLockRelease(pg_tracing_shared_state->lock);
		return;
	}

	/* Do marshalling within marshal memory context */
	MemoryContextSwitchTo(marshal_mem_ctx);
	/* Build the json context */
	build_json_context(&json_ctx, qbuffer, qbuffer_size, shared_spans);
	marshal_spans_to_json(&json_ctx);
	MemoryContextSwitchTo(otel_exporter_mem_ctx);

	/* TODO: Only drop if we manage to send spans */
	drop_all_spans_locked();

	pg_tracing_shared_state->stats.last_consume = GetCurrentTimestamp();
	LWLockRelease(pg_tracing_shared_state->lock);

	if (json_ctx.str->len > 0)
	{
		CURLcode	ret;

		elog(INFO, "Sending %d spans to %s", num_spans, octx->endpoint);
		ret = send_json_trace(octx, json_ctx.str->data);

		/* TODO: Switch to atomic values for stats? */
		LWLockAcquire(pg_tracing_shared_state->lock, LW_EXCLUSIVE);
		if (ret == CURLE_OK)
		{
			pg_tracing_shared_state->stats.otel_sent_spans += num_spans;
		}
		else
		{
			ereport(WARNING, errmsg("curl_easy_perform() failed: %s\n",
									curl_easy_strerror(ret)));
			pg_tracing_shared_state->stats.otel_failures++;
		}
		LWLockRelease(pg_tracing_shared_state->lock);
	}

	/* Json was pushed, we can clear the memory used for marshalling */
	MemoryContextReset(marshal_mem_ctx);
	free(qbuffer);
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
	otelContext.headers = NULL;
	otelContext.curl = NULL;
	otelContext.endpoint = endpoint;
	otelContext.naptime = naptime;
	otelContext.connect_timeout_ms = connect_timeout_ms;

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
	otelContext.headers = curl_slist_append(otelContext.headers, "Content-Type: application/json");

	while (!ShutdownRequestPending)
	{
		int			rc;

		/* Clean the latch and wait for the next event */
		ResetLatch(MyLatch);
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   otelContext.naptime,
					   PG_WAIT_EXTENSION);

		/* Send spans if we have any */
		if (rc & WL_TIMEOUT)
			send_spans_to_otel_collector(&otelContext);
	}

	/* Curl cleanup */
	curl_slist_free_all(otelContext.headers);
	otelContext.headers = NULL;
	if (otelContext.curl)
	{
		curl_easy_cleanup(otelContext.curl);
		otelContext.curl = NULL;
	}
	curl_global_cleanup();
}
