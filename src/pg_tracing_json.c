/*-------------------------------------------------------------------------
 *
 * pg_tracing_json.c
 * 		pg_tracing json export functions.
 *
 * IDENTIFICATION
 *	  src/pg_tracing_json.c
 *
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
	appendStringInfo(str, "\"%s\": %" INT64_MODIFIER "d", label, value);
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

	char	   *ts_nano = psprintf("%" INT64_MODIFIER "u000", result);

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
append_resource(StringInfo str)
{
	appendStringInfo(str, "\"resource\":");
	appendStringInfo(str, "{\"attributes\": [");
	/* TODO: Make the service name configurable */
	append_attribute_string(str, "service.name", "PostgreSQL_Server", false);
	appendStringInfo(str, "]}");
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
append_span_attributes(StringInfo str, const Span * span)
{
	appendStringInfo(str, "\"attributes\": [");

	if (span->type >= SPAN_NODE && span->type <= SPAN_TOP_UNKNOWN)
		append_node_counters(str, &span->node_counters);
	if (span->type >= SPAN_TOP_SELECT && span->type <= SPAN_TOP_UNKNOWN)
		append_plan_counters(str, &span->plan_counters);

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

	operation_name = get_operation_name(span, json_ctx->qbuffer, json_ctx->qbuffer_size);

	pg_snprintf(trace_id, 33, INT64_HEX_FORMAT INT64_HEX_FORMAT,
				span->trace_id.traceid_left,
				span->trace_id.traceid_right);
	pg_snprintf(parent_id, 17, INT64_HEX_FORMAT, span->parent_id);
	pg_snprintf(span_id, 17, INT64_HEX_FORMAT, span->span_id);

	appendStringInfoChar(str, '{');
	append_text_field(str, "traceId", trace_id, true);
	append_text_field(str, "spanId", span_id, true);
	append_text_field(str, "parentSpanId", parent_id, true);
	append_text_field(str, "name", operation_name, true);
	append_int_field(str, "kind", SPAN_KIND_SERVER, true);
	append_nano_timestamp(str, "startTimeUnixNano", span->start, true);
	append_nano_timestamp(str, "endTimeUnixNano", span->end, true);
	append_span_attributes(str, span);
	appendStringInfoChar(str, '}');
}

static void
append_scope_spans(const JsonContext * json_ctx, const pgTracingSpans * spans)
{
	appendStringInfo(json_ctx->str, "\"scopeSpans\":[");
	appendStringInfo(json_ctx->str, "{\"spans\": [");
	for (int i = 0; i < shared_spans->end; i++)
	{
		append_span(json_ctx, spans->spans + i);
		if (i + 1 < shared_spans->end)
			appendStringInfoChar(json_ctx->str, ',');
	}
	appendStringInfo(json_ctx->str, "]}]");
}

/*
 * Marshal spans json compatible for otel http endpoint
 */
void
marshal_spans_to_json(const JsonContext * json_ctx, const pgTracingSpans * spans)
{
	appendStringInfo(json_ctx->str, "{\"resourceSpans\": [{");
	append_resource(json_ctx->str);
	appendStringInfoChar(json_ctx->str, ',');
	append_scope_spans(json_ctx, spans);

	appendStringInfo(json_ctx->str, "}]}");
}
