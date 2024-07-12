/*-------------------------------------------------------------------------
 *
 * pg_tracing_query_process.c
 * 		pg_tracing query processing functions.
 *
 * IDENTIFICATION
 *	  src/pg_tracing_explain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "utils/memutils.h"
#include "parser/scanner.h"
#include "pg_tracing.h"

/*
 * Convert parse traceparent error code to string
 */
char *
parse_code_to_err(ParseTraceparentErr err)
{
	switch (err)
	{
		case PARSE_OK:
			return "No error";
		case PARSE_INCORRECT_SIZE:
			return "incorrect size";
		case PARSE_NO_TRACEPARENT_FIELD:
			return "No traceparent field found";
		case PARSE_INCORRECT_TRACEPARENT_SIZE:
			return "Traceparent field doesn't have the correct size";
		case PARSE_INCORRECT_FORMAT:
			return "Incorrect traceparent format";
	}
	return "Unknown error";
}

/*
 * Check if we have a comment that could store SQLComment information at the
 * start or end of the query.
 * Returns 0 if there's possible comment at the beginning, 1 at the end and -1
 * if no comments were detected.
 */
static int
has_possible_sql_comment(const char *query)
{
	int			query_len = strlen(query);
	int			end = query_len - 1;

	/*
	 * We need at least a comment with 'traceparent' information.
	 * "traceparent" + "=" + '' -> 13 characters.
	 * 00-00000000000000000000000000000009-0000000000000005-01 -> 55
	 * characters. Comments start and ends -> 4 characters.
	 */
	if (query_len < 55 + 13 + 4)
		return -1;

	/* Check if we have comment at the beginning */
	if (query[0] == '/' && query[1] == '*')
		return 0;

	/* Ignore the ; at the end if present */
	if (query[end] == ';')
		end--;

	/* Check end of comment at the end of the query */
	if (query[end - 1] == '*' && query[end] == '/')
		return 1;

	return -1;
}

/*
 * Parse trace parent values.
 * The expected format for traceparent is: version-traceid-parentid-sampled
 * Example: 00-00000000000000000000000000000009-0000000000000005-01
 */
static ParseTraceparentErr
parse_traceparent_value(Traceparent * traceparent, const char *traceparent_str)
{
	char	   *traceid_left;
	char	   *endptr;

	reset_traceparent(traceparent);

	/* Check that '-' are at the expected places */
	if (traceparent_str[2] != '-' ||
		traceparent_str[35] != '-' ||
		traceparent_str[52] != '-' ||
		traceparent_str[55] != '\'')
		return PARSE_INCORRECT_FORMAT;

	/* Parse traceparent parameters */
	errno = 0;

	traceid_left = pnstrdup(&traceparent_str[3], 16);
	traceparent->trace_id.traceid_left = strtou64(traceid_left, &endptr, 16);
	pfree(traceid_left);
	traceparent->trace_id.traceid_right = strtou64(&traceparent_str[3 + 16], &endptr, 16);

	if (endptr != traceparent_str + 35 || errno)
		return PARSE_INCORRECT_FORMAT;
	traceparent->parent_id = strtou64(&traceparent_str[36], &endptr, 16);
	if (endptr != traceparent_str + 52 || errno)
		return PARSE_INCORRECT_FORMAT;
	traceparent->sampled = strtol(&traceparent_str[53], &endptr, 16);
	if (endptr != traceparent_str + 55 || errno)
		/* Just to be sure, reset sampled on error */
		traceparent->sampled = 0;

	return PARSE_OK;
}

/*
 * Extract trace context from a SQLCommenter value in a query
 *
 * We're expecting the query to start or end with a SQLComment containing the
 * traceparent parameter
 * "/\*traceparent='00-00000000000000000000000000000009-0000000000000005-01'*\/ SELECT 1;"
 * Or "SELECT 1 /\*traceparent='00-00000000000000000000000000000009-0000000000000005-01'*\/;"
 */
void
extract_trace_context_from_query(Traceparent * traceparent,
								 const char *query)
{
	const char *start_sqlcomment = NULL;
	const char *end_sqlcomment;

	int			possible_comment = has_possible_sql_comment(query);

	if (possible_comment == 0)
	{
		/* We have a possible comment at the beginning */
		start_sqlcomment = query;
		end_sqlcomment = strstr(query, "*/");
		if (end_sqlcomment == NULL)
			return;
	}
	else if (possible_comment == 1)
	{
		/* We have a possible comment at the end */
		start_sqlcomment = strstr(query, "/*");
		if (start_sqlcomment == NULL)
			return;
		end_sqlcomment = strstr(start_sqlcomment, "*/");
		if (end_sqlcomment == NULL)
			return;
	}
	else
	{
		/* No sqlcomment, bail out */
		return;
	}

	parse_trace_context(traceparent, start_sqlcomment,
						end_sqlcomment - start_sqlcomment);
}

/*
 * Parse trace context value.
 * We're only interested in traceparent value to get the trace id, parent id and sampled flag.
 */
ParseTraceparentErr
parse_trace_context(Traceparent * traceparent, const char *trace_context_str,
					int trace_context_len)
{
	const char *traceparent_str;
	const char *end_trace_context;

	end_trace_context = trace_context_str + trace_context_len;

	/*
	 * Locate traceparent parameter and make sure it has the expected size
	 * "traceparent" + "=" + ' -> 13 characters
	 * 00-00000000000000000000000000000009-0000000000000005-01 -> 55 ' -> 1
	 * character characters
	 */
	traceparent_str = strstr(trace_context_str, "traceparent='");
	if (traceparent_str == NULL)
		return PARSE_NO_TRACEPARENT_FIELD;

	if (traceparent_str > end_trace_context ||
		end_trace_context - traceparent_str < 55 + 13 + 1)
		return PARSE_INCORRECT_TRACEPARENT_SIZE;

	/* Move to the start of the traceparent values */
	traceparent_str = traceparent_str + 13;
	/* And parse it */
	return parse_traceparent_value(traceparent, traceparent_str);
}

/*
 * comp_location: comparator for qsorting LocationLen structs by location
 */
static int
comp_location(const void *a, const void *b)
{
	int			l = ((const LocationLen *) a)->location;
	int			r = ((const LocationLen *) b)->location;

	if (l < r)
		return -1;
	else if (l > r)
		return +1;
	else
		return 0;
}


/*
 * Normalise query: Comments are removed, Constants are replaced by $x, all tokens
 * are separated by a single space.
 * Parameters are put in the trace_txt as null terminated strings.
 */
const char *
normalise_query_parameters(const JumbleState *jstate, const char *query,
						   int query_loc, int *query_len_p,
						   StringInfo trace_text, int *num_parameters)
{
	char	   *norm_query;
	int			query_len = *query_len_p;
	int			len_parameter;
	int			norm_query_buflen,	/* Space allowed for norm_query */
				n_quer_loc = 0;
	LocationLen *locs;
	core_yyscan_t yyscanner;
	core_yy_extra_type yyextra;
	core_YYSTYPE yylval;
	YYLTYPE		yylloc;
	int			current_loc = 0;

	if (query_loc == -1)
	{
		/* If query location is unknown, distrust query_len as well */
		query_loc = 0;
		query_len = strlen(query);
	}
	else
	{
		/* Length of 0 (or -1) means "rest of string" */
		if (query_len <= 0)
			query_len = strlen(query);
		else
			Assert(query_len <= strlen(query));
	}

	norm_query_buflen = query_len + jstate->clocations_count * 10;
	Assert(norm_query_buflen > 0);
	locs = jstate->clocations;

	/* Allocate result buffer */
	norm_query = palloc(norm_query_buflen + 1);

	if (jstate->clocations_count > 1)
		qsort(jstate->clocations, jstate->clocations_count,
			  sizeof(LocationLen), comp_location);

	/* initialize the flex scanner --- should match raw_parser() */
	yyscanner = scanner_init(query + query_loc,
							 &yyextra,
							 &ScanKeywords,
							 ScanKeywordTokens);

	for (;;)
	{
		int			loc = locs[current_loc].location;
		int			tok;

		loc -= query_loc;

		tok = core_yylex(&yylval, &yylloc, yyscanner);

		/*
		 * We should not hit end-of-string, but if we do, behave sanely
		 */
		if (tok == 0)
			break;				/* out of inner for-loop */
		if (yylloc > query_len)
			break;

		/*
		 * We should find the token position exactly, but if we somehow run
		 * past it, work with that.
		 */
		if (current_loc < jstate->clocations_count && yylloc >= loc)
		{
			if (query[loc] == '-')
			{
				/*
				 * It's a negative value - this is the one and only case where
				 * we replace more than a single token.
				 *
				 * Do not compensate for the core system's special-case
				 * adjustment of location to that of the leading '-' operator
				 * in the event of a negative constant.  It is also useful for
				 * our purposes to start from the minus symbol.  In this way,
				 * queries like "select * from foo where bar = 1" and "select *
				 * from foo where bar = -2" will have identical normalized
				 * query strings.
				 */
				if (trace_text)
					appendStringInfoChar(trace_text, '-');
				tok = core_yylex(&yylval, &yylloc, yyscanner);
				if (tok == 0)
					break;		/* out of inner for-loop */
			}
			if (yylloc > 0 && isspace(yyextra.scanbuf[yylloc - 1]) && n_quer_loc > 0)
				norm_query[n_quer_loc++] = yyextra.scanbuf[yylloc - 1];

			/* Append the current parameter $x in the normalised query */
			n_quer_loc += sprintf(norm_query + n_quer_loc, "$%d",
								  current_loc + 1 + jstate->highest_extern_param_id);

			if (trace_text)
			{
				/* Add paramater's value to the trace_text */
				len_parameter = strlen(yyextra.scanbuf + yylloc);
				appendBinaryStringInfo(trace_text, yyextra.scanbuf + yylloc, len_parameter);
				appendStringInfoChar(trace_text, '\0');
				*num_parameters += 1;
			}

			current_loc++;
		}
		else
		{
			int			to_copy;

			if (yylloc > 0 && isspace(yyextra.scanbuf[yylloc - 1]) && n_quer_loc > 0)
				norm_query[n_quer_loc++] = yyextra.scanbuf[yylloc - 1];

			to_copy = strlen(yyextra.scanbuf + yylloc);
			Assert(n_quer_loc + to_copy < norm_query_buflen + 1);
			memcpy(norm_query + n_quer_loc, yyextra.scanbuf + yylloc, to_copy);
			n_quer_loc += to_copy;
		}
	}
	scanner_finish(yyscanner);

	*query_len_p = n_quer_loc;
	norm_query[n_quer_loc] = '\0';
	return norm_query;
}

/*
 * Normalise query: tokens will be separated by a single space
 */
const char *
normalise_query(const char *query, int query_loc, int *query_len_p)
{
	char	   *norm_query;
	int			query_len = *query_len_p;
	int			norm_query_buflen = query_len;
	int			n_quer_loc = 0;
	core_yyscan_t yyscanner;
	core_yy_extra_type yyextra;
	core_YYSTYPE yylval;
	YYLTYPE		yylloc;

	/* Allocate result buffer */
	norm_query = palloc(norm_query_buflen + 2);

	/* initialize the flex scanner --- should match raw_parser() */
	yyscanner = scanner_init(query + query_loc, &yyextra, &ScanKeywords,
							 ScanKeywordTokens);
	for (;;)
	{
		int			tok;
		int			to_copy;

		tok = core_yylex(&yylval, &yylloc, yyscanner);

		if (tok == 0)
			break;				/* out of inner for-loop */
		if (yylloc > query_len)
			break;

		if (yylloc > 0 && isspace(yyextra.scanbuf[yylloc - 1]) && n_quer_loc > 0)
			norm_query[n_quer_loc++] = yyextra.scanbuf[yylloc - 1];
		to_copy = strlen(yyextra.scanbuf + yylloc);
		Assert(n_quer_loc + to_copy < norm_query_buflen + 2);
		memcpy(norm_query + n_quer_loc, yyextra.scanbuf + yylloc, to_copy);
		n_quer_loc += to_copy;
	}
	scanner_finish(yyscanner);

	*query_len_p = n_quer_loc;
	norm_query[n_quer_loc] = '\0';
	return norm_query;
}
