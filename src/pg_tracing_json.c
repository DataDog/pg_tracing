/*-------------------------------------------------------------------------
 *
 * pg_tracing_json.c
 * 		pg_tracing json export functions.
 *
 * Generate a OTLP/JSON from spans. Spans are scoped by span type (Planner, ExecutorRun, SeqScan...).
 * Example of generated json:
 * {"resourceSpans" : [{
 *     "resource" : {"attributes" : [{ "key" : "service.name", "value" : {"stringValue" : "PostgreSQL_Server"}}]},
 *     "scopeSpans" : [
 *       {"scope" : {"name" : "Planner"},
 *        "spans" : [{ "attributes" :[{ "key" : "query.query_id", "value" : { "intValue" : 6865378226349601843 }}],
 *                     "endTimeUnixNano" : "1720075532201401000",
 *                     "kind" : 2,
 *                     "name" : "Planner",
 *                     "parentSpanId" : "6a3f7a0e50012fe0",
 *                     "spanId" : "7ebb2b1481e2f246",
 *                     "startTimeUnixNano" : "1720075532201271000",
 *                     "traceId" : "daed1ea3e0526f4cdea3e41d007d5b91"}]},
 *       {"scope" : {"name" : "Select query"},
 *        "spans" : [{"attributes" : [{"key" : "node.rows","value" : {"intValue" : 1}},
 *                                    {"key" : "query.parameters","value" : {"stringValue" : "$1 = 1"}}],
 *                    "endTimeUnixNano" : "1720075532201772000",
 *                    "kind" : 2,
 *                    "name" : "select $1;",
 *                    "parentSpanId" : "dea3e41d007d5b91",
 *                    "spanId" : "6a3f7a0e50012fe0",
 *                    "startTimeUnixNano" : "1720075532201227000",
 *                    "traceId" : "daed1ea3e0526f4cdea3e41d007d5b91"
 *             }]}
 *          ]}]}
 *
 * IDENTIFICATION
 *	  src/pg_tracing_json.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_tracing.h"
#include "utils/timestamp.h"
#include "utils/json.h"

/*
 * From https://github.com/open-telemetry/opentelemetry-proto/blob/v1.3.1/opentelemetry/proto/trace/v1/trace.proto#L159-L161
 */
#define SPAN_KIND_SERVER 2
#define STATUS_CODE_OK 1
#define STATUS_CODE_ERROR 2

static void
append_text_field(StringInfo str, const char *label, const char *value, bool add_comma)
{
	escape_json(str, label);
	appendStringInfoChar(str, ':');
	escape_json(str, value);
	if (add_comma)
		appendStringInfoChar(str, ',');
}

static void
append_int_field(StringInfo str, const char *label, int64 value, bool add_comma)
{
	appendStringInfo(str, "\"%s\": " INT64_FORMAT, label, value);
	if (add_comma)
		appendStringInfoChar(str, ',');
}

static void
append_double_field(StringInfo str, const char *label, double value, bool add_comma)
{
	appendStringInfo(str, "\"%s\": %f", label, value);
	if (add_comma)
		appendStringInfoChar(str, ',');
}

static void
append_nano_timestamp(StringInfo str, const char *label, TimestampTz value, bool add_comma)
{
	Timestamp	epoch = SetEpochTimestamp();
	int64		result = value - epoch;

	char	   *ts_nano = psprintf(UINT64_FORMAT "000", result);

	escape_json(str, label);
	appendStringInfoChar(str, ':');
	escape_json(str, ts_nano);
	if (add_comma)
		appendStringInfoChar(str, ',');
}

static void
append_any_value_start(StringInfo str, const char *key)
{
	appendStringInfoChar(str, '{');
	append_text_field(str, "key", key, false);
	appendStringInfoChar(str, ',');
	appendStringInfo(str, "\"value\":{");
}

static void
append_attribute_string(StringInfo str, const char *key, const char *value, bool add_comma)
{
	append_any_value_start(str, key);
	append_text_field(str, "stringValue", value, false);
	appendStringInfo(str, "}}");
	if (add_comma)
		appendStringInfoChar(str, ',');
}

static void
append_attribute_int(StringInfo str, const char *key, int64 value, bool skip_if_zero, bool add_comma)
{
	if (value == 0 && skip_if_zero)
		return;
	append_any_value_start(str, key);
	append_int_field(str, "intValue", value, false);
	appendStringInfo(str, "}}");
	if (add_comma)
		appendStringInfoChar(str, ',');
}

static void
append_attribute_double(StringInfo str, const char *key, double value, bool skip_if_zero, bool add_comma)
{
	if (value == 0 && skip_if_zero)
		return;
	append_any_value_start(str, key);
	append_double_field(str, "doubleValue", value, false);
	appendStringInfo(str, "}}");
	if (add_comma)
		appendStringInfoChar(str, ',');
}

static void
append_resource(JsonContext * json_ctx)
{
	appendStringInfo(json_ctx->str, "\"resource\":");
	appendStringInfo(json_ctx->str, "{\"attributes\": [");
	append_attribute_string(json_ctx->str, "service.name", json_ctx->service_name, false);
	appendStringInfo(json_ctx->str, "]}");
}

static void
append_array_value_field(StringInfo str, const char *key, const char *value,
						 int num_values, bool add_comma)
{
	const char *cursor = value;

	append_any_value_start(str, key);
	appendStringInfo(str, "\"arrayValue\":{");
	appendStringInfo(str, "\"values\":[");

	for (int i = 0; i < num_values; i++)
	{
		int			len_val = strlen(cursor);

		appendStringInfoChar(str, '{');
		append_text_field(str, "stringValue", cursor, false);
		appendStringInfoChar(str, '}');
		if (i != num_values - 1)
			appendStringInfoChar(str, ',');
		/* Advance to next parameter, including null terminating the string */
		cursor += len_val + 1;
	}
	appendStringInfo(str, "]}}}");

	if (add_comma)
		appendStringInfoChar(str, ',');
}

static void
append_plan_counters(StringInfo str, const PlanCounters * plan_counters)
{
	append_attribute_double(str, "plan.cost.startup", plan_counters->startup_cost, true, true);
	append_attribute_double(str, "plan.cost.total", plan_counters->total_cost, true, true);
	append_attribute_double(str, "plan.rows", plan_counters->plan_rows, true, true);
	append_attribute_int(str, "plan.width", plan_counters->plan_width, true, true);
}

static void
append_node_counters(StringInfo str, const NodeCounters * node_counters)
{
	double		blk_read_time,
				blk_write_time,
				temp_blk_read_time,
				temp_blk_write_time;
	double		generation_counter,
				inlining_counter,
				optimization_counter,
				emission_counter;
	int64		jit_created_functions;

	append_attribute_int(str, "node.rows", node_counters->rows, true, true);
	append_attribute_int(str, "node.nloops", node_counters->nloops, true, true);

	/* Block usage */
	append_attribute_int(str, "blocks.shared.hit", node_counters->buffer_usage.shared_blks_hit, true, true);
	append_attribute_int(str, "blocks.shared.read", node_counters->buffer_usage.shared_blks_read, true, true);
	append_attribute_int(str, "blocks.shared.dirtied", node_counters->buffer_usage.shared_blks_dirtied, true, true);
	append_attribute_int(str, "blocks.shared.written", node_counters->buffer_usage.shared_blks_written, true, true);

	append_attribute_int(str, "blocks.local.hit", node_counters->buffer_usage.local_blks_hit, true, true);
	append_attribute_int(str, "blocks.local.read", node_counters->buffer_usage.local_blks_read, true, true);
	append_attribute_int(str, "blocks.local.dirtied", node_counters->buffer_usage.local_blks_dirtied, true, true);
	append_attribute_int(str, "blocks.local.written", node_counters->buffer_usage.local_blks_written, true, true);

#if PG_VERSION_NUM >= 170000
	blk_read_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.shared_blk_read_time);
	blk_write_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.shared_blk_write_time);
#else
	blk_read_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.blk_read_time);
	blk_write_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.blk_write_time);
#endif
	append_attribute_double(str, "blocks.io.read_time", blk_read_time, true, true);
	append_attribute_double(str, "blocks.io.write_time", blk_write_time, true, true);

	temp_blk_read_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.temp_blk_read_time);
	temp_blk_write_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.temp_blk_write_time);
	append_attribute_double(str, "temp_blocks.io.read_time", temp_blk_read_time, true, true);
	append_attribute_double(str, "temp_blocks.io.write_time", temp_blk_write_time, true, true);
	append_attribute_int(str, "temp_blocks.read", node_counters->buffer_usage.temp_blks_read, true, true);
	append_attribute_int(str, "temp_blocks.written", node_counters->buffer_usage.temp_blks_written, true, true);

	/* WAL usage */
	append_attribute_int(str, "wal.records", node_counters->wal_usage.wal_records, true, true);
	append_attribute_int(str, "wal.fpi", node_counters->wal_usage.wal_fpi, true, true);
	append_attribute_int(str, "wal.bytes", node_counters->wal_usage.wal_bytes, true, true);

	/* JIT usage */
	generation_counter = INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.generation_counter);
	inlining_counter = INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.inlining_counter);
	optimization_counter = INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.optimization_counter);
	emission_counter = INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.emission_counter);
	jit_created_functions = node_counters->jit_usage.created_functions;
	append_attribute_int(str, "jit.created_functions", jit_created_functions, true, true);
	append_attribute_double(str, "jit.generation_counter", generation_counter, true, true);
	append_attribute_double(str, "jit.inlining_counter", inlining_counter, true, true);
	append_attribute_double(str, "jit.optimization_counter", optimization_counter, true, true);
	append_attribute_double(str, "jit.emission_counter", emission_counter, true, true);
}

static void
append_span_status_code(StringInfo str, const Span * span)
{
	int			code = STATUS_CODE_OK;

	appendStringInfo(str, "\"status\":{");
	if (span->sql_error_code > 0)
	{
		code = STATUS_CODE_ERROR;
		append_text_field(str, "message", psprintf("SQLError: %s", unpack_sql_state(span->sql_error_code)), true);
	}
	append_int_field(str, "code", code, false);
	appendStringInfo(str, "},");
}

static void
append_span_attributes(const JsonContext * json_ctx, const Span * span)
{
	StringInfo	str = json_ctx->str;

	appendStringInfo(str, "\"attributes\": [");

	if ((span->type >= SPAN_NODE && span->type <= SPAN_TOP_UNKNOWN)
		|| span->type == SPAN_PLANNER)
	{
		append_node_counters(str, &span->node_counters);
		append_plan_counters(str, &span->plan_counters);

		append_attribute_int(str, "query.startup", span->startup, true, true);
		if (span->parameter_offset != -1)
			append_array_value_field(str, "query.parameters",
									 json_ctx->spans_str + span->parameter_offset, span->num_parameters, true);

		if (span->deparse_info_offset != -1)
			append_attribute_string(str, "query.deparse_info",
									json_ctx->spans_str + span->deparse_info_offset, true);
	}

	if (span->sql_error_code > 0)
		append_attribute_string(str, "query.sql_error_code", unpack_sql_state(span->sql_error_code), true);
	append_attribute_int(str, "query.query_id", span->query_id, false, true);
	append_attribute_int(str, "query.subxact_count", span->subxact_count, true, true);

	append_attribute_int(str, "backend.pid", span->be_pid, false, true);
	append_attribute_int(str, "backend.user_id", span->user_id, false, true);
	append_attribute_int(str, "backend.database_id", span->database_id, false, false);

	appendStringInfoChar(str, ']');
}

static void
append_span(const JsonContext * json_ctx, const Span * span)
{
	char		trace_id[33];
	char		parent_id[17];
	char		span_id[17];
	const char *operation_name;
	StringInfo	str = json_ctx->str;

	operation_name = get_operation_name(span);

	pg_snprintf(trace_id, 33, UINT64_HEX_PADDED_FORMAT UINT64_HEX_PADDED_FORMAT,
				span->trace_id.traceid_left,
				span->trace_id.traceid_right);
	pg_snprintf(parent_id, 17, UINT64_HEX_PADDED_FORMAT, span->parent_id);
	pg_snprintf(span_id, 17, UINT64_HEX_PADDED_FORMAT, span->span_id);

	appendStringInfoChar(str, '{');
	append_text_field(str, "traceId", trace_id, true);
	append_text_field(str, "spanId", span_id, true);
	append_text_field(str, "parentSpanId", parent_id, true);
	append_text_field(str, "name", operation_name, true);
	append_int_field(str, "kind", SPAN_KIND_SERVER, true);
	append_nano_timestamp(str, "startTimeUnixNano", span->start, true);
	append_nano_timestamp(str, "endTimeUnixNano", span->end, true);
	append_span_status_code(str, span);
	append_span_attributes(json_ctx, span);
	appendStringInfoChar(str, '}');
}

static void
append_scope(const JsonContext * json_ctx, SpanType span_type)
{
	appendStringInfo(json_ctx->str, "\"scope\":{");
	append_text_field(json_ctx->str, "name", span_type_to_str(span_type), false);
	appendStringInfo(json_ctx->str, "},");
}

static void
append_scope_spans(const JsonContext * json_ctx, SpanType span_type)
{
	appendStringInfoChar(json_ctx->str, '{');
	append_scope(json_ctx, span_type);
	appendStringInfo(json_ctx->str, "\"spans\": [");
	for (int i = 0; i < json_ctx->span_type_count[span_type]; i++)
	{
		const		Span *span = json_ctx->span_type_to_spans[span_type][i];

		append_span(json_ctx, span);
		if (i + 1 < json_ctx->span_type_count[span_type])
			appendStringInfoChar(json_ctx->str, ',');
	}
	appendStringInfo(json_ctx->str, "]}");
}

static void
append_scope_spans_array(const JsonContext * json_ctx)
{
	bool		first = true;

	appendStringInfo(json_ctx->str, "\"scopeSpans\":[");
	for (int span_type = 0; span_type < NUM_SPAN_TYPE; span_type++)
	{
		if (json_ctx->span_type_count[span_type] == 0)
			continue;
		if (!first)
			appendStringInfoChar(json_ctx->str, ',');
		else
			first = false;
		append_scope_spans(json_ctx, span_type);
	}
	appendStringInfo(json_ctx->str, "]");
}

/*
 * Build the span_type_to_spans array in JsonContext
 *
 * With OTLP/JSON, spans are grouped by scopes. We use span_type (Planner, Select Query...)
 * as scope. We need to group the spans by span_type before generating the json.
 */
static void
aggregate_span_by_type(JsonContext * json_ctx, const pgTracingSpans * pgTracingSpans)
{
	int			span_type_to_current_pos[NUM_SPAN_TYPE] = {0};

	/* Count how many spans per type we have */
	for (int i = 0; i < pgTracingSpans->end; i++)
		json_ctx->span_type_count[pgTracingSpans->spans[i].type]++;

	/* Allocate necessary space */
	for (int span_type = 0; span_type < NUM_SPAN_TYPE; span_type++)
	{
		int			num_spans = json_ctx->span_type_count[span_type];

		if (num_spans == 0)
			continue;
		json_ctx->span_type_to_spans[span_type] = palloc(sizeof(Span *) * num_spans);
	}

	/* Group spans by type */
	for (int i = 0; i < pgTracingSpans->end; i++)
	{
		const		Span *span = &pgTracingSpans->spans[i];
		int			index = span_type_to_current_pos[span->type]++;

		json_ctx->span_type_to_spans[span->type][index] = span;
	}
}

/*
 * Prepare json context for json marshalling
 */
void
build_json_context(JsonContext * json_ctx, const pgTracingSpans * pgTracingSpans, const char *spans_str, int num_spans)
{
	json_ctx->str = makeStringInfo();
	memset(json_ctx->span_type_count, 0, sizeof(json_ctx->span_type_count));
	memset(json_ctx->span_type_to_spans, 0, sizeof(json_ctx->span_type_to_spans));
	json_ctx->spans_str = spans_str;
	json_ctx->num_spans = num_spans;
	json_ctx->service_name = pg_tracing_otel_service_name;

	aggregate_span_by_type(json_ctx, pgTracingSpans);
}

/*
 * Marshal spans json compatible for otel http endpoint
 */
void
marshal_spans_to_json(JsonContext * json_ctx)
{
	appendStringInfo(json_ctx->str, "{\"resourceSpans\": [{");
	append_resource(json_ctx);
	appendStringInfoChar(json_ctx->str, ',');
	append_scope_spans_array(json_ctx);
	appendStringInfo(json_ctx->str, "}]}");
}
