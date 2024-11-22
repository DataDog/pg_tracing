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
 * Given a valid SQL string and an array of constant-location records,
 * fill in the textual lengths of those constants.
 *
 * The constants may use any allowed constant syntax, such as float literals,
 * bit-strings, single-quoted strings and dollar-quoted strings.  This is
 * accomplished by using the public API for the core scanner.
 *
 * It is the caller's job to ensure that the string is a valid SQL statement
 * with constants at the indicated locations.  Since in practice the string
 * has already been parsed, and the locations that the caller provides will
 * have originated from within the authoritative parser, this should not be
 * a problem.
 *
 * Duplicate constant pointers are possible, and will have their lengths
 * marked as '-1', so that they are later ignored.  (Actually, we assume the
 * lengths were initialized as -1 to start with, and don't change them here.)
 *
 * If query_loc > 0, then "query" has been advanced by that much compared to
 * the original string start, so we need to translate the provided locations
 * to compensate.  (This lets us avoid re-scanning statements before the one
 * of interest, so it's worth doing.)
 *
 * N.B. There is an assumption that a '-' character at a Const location begins
 * a negative numeric constant.  This precludes there ever being another
 * reason for a constant to start with a '-'.
 */
static void
fill_in_constant_lengths(const JumbleState *jstate, const char *query,
						 int query_loc, int *sub_query_loc)
{
	LocationLen *locs;
	core_yyscan_t yyscanner;
	core_yy_extra_type yyextra;
	core_YYSTYPE yylval;
	YYLTYPE		yylloc;
	int			last_loc = -1;
	int			i;

	/*
	 * Sort the records by location so that we can process them in order while
	 * scanning the query text.
	 */
	if (jstate->clocations_count > 1)
		qsort(jstate->clocations, jstate->clocations_count,
			  sizeof(LocationLen), comp_location);
	locs = jstate->clocations;

	/* initialize the flex scanner --- should match raw_parser() */
	yyscanner = scanner_init(query,
							 &yyextra,
							 &ScanKeywords,
							 ScanKeywordTokens);

	/* we don't want to re-emit any escape string warnings */
	yyextra.escape_string_warning = false;

	/* Search for each constant, in sequence */
	for (i = 0; i < jstate->clocations_count; i++)
	{
		int			loc = locs[i].location;
		int			tok;

		/* Adjust recorded location if we're dealing with partial string */
		loc -= query_loc;

		Assert(loc >= 0);

		if (loc <= last_loc)
			continue;			/* Duplicate constant, ignore */

		/* Lex tokens until we find the desired constant */
		for (;;)
		{
			tok = core_yylex(&yylval, &yylloc, yyscanner);

			/* We should not hit end-of-string, but if we do, behave sanely */
			if (tok == 0)
				break;			/* out of inner for-loop */

			if (*sub_query_loc == -1)
				*sub_query_loc = yylloc;

			/*
			 * We should find the token position exactly, but if we somehow
			 * run past it, work with that.
			 */
			if (yylloc >= loc)
			{
				if (query[loc] == '-')
				{
					/*
					 * It's a negative value - this is the one and only case
					 * where we replace more than a single token.
					 *
					 * Do not compensate for the core system's special-case
					 * adjustment of location to that of the leading '-'
					 * operator in the event of a negative constant.  It is
					 * also useful for our purposes to start from the minus
					 * symbol.  In this way, queries like "select * from foo
					 * where bar = 1" and "select * from foo where bar = -2"
					 * will have identical normalized query strings.
					 */
					tok = core_yylex(&yylval, &yylloc, yyscanner);
					if (tok == 0)
						break;	/* out of inner for-loop */
				}

				/*
				 * We now rely on the assumption that flex has placed a zero
				 * byte after the text of the current token in scanbuf.
				 */
				locs[i].length = strlen(yyextra.scanbuf + loc);
				break;			/* out of inner for-loop */
			}
		}

		/* If we hit end-of-string, give up, leaving remaining lengths -1 */
		if (tok == 0)
			break;

		last_loc = loc;
	}

	scanner_finish(yyscanner);
}

/*
 * Normalise query: Comments are removed, Constants are replaced by $x, all tokens
 * are separated by a single space.
 * Parameters are put in the trace_txt as null terminated strings.
 */
const char *
normalise_query_parameters(const SpanContext * span_context, Span * span,
						   int *query_len_p)
{
	const char *sub_query;
	int			query_loc = span_context->query->stmt_location; /* Query location in the
																 * complete query_text */
	int			query_len = span_context->query->stmt_len;	/* Query length in the
															 * complete query_text */
	const JumbleState *jstate = span_context->jstate;
	char	   *norm_query;
	int			i,
				sub_query_loc = -1, /* Location in the confined sub query */
				norm_query_buflen,	/* Space allowed for norm_query */
				len_to_wrt,		/* Length (in bytes) to write */
				n_quer_loc = 0; /* Normalized query byte location */
	bool		extract_parameters = span_context->max_parameter_size > 0;


	/*
	 * Confine our attention to the relevant part of the string, if the query
	 * is a portion of a multi-statement source string, and update query
	 * location and length if needed.
	 */
	sub_query = CleanQuerytext(span_context->query_text, &query_loc, &query_len);
	if (query_len == 0)
		return "";

	/*
	 * Get constants' lengths (core system only gives us locations).  Note
	 * this also ensures the items are sorted by location.
	 */
	fill_in_constant_lengths(jstate, sub_query, query_loc, &sub_query_loc);

	/*
	 * Save the current position in parameters buffer for the eventual
	 * parameter values
	 */
	if (extract_parameters)
		span->parameter_offset = span_context->parameters_buffer->len;

	/*
	 * Allow for $n symbols to be longer than the constants they replace.
	 * Constants must take at least one byte in text form, while a $n symbol
	 * certainly isn't more than 11 bytes, even if n reaches INT_MAX.  We
	 * could refine that limit based on the max value of n for the current
	 * query, but it hardly seems worth any extra effort to do so.
	 */
	norm_query_buflen = query_len + jstate->clocations_count * 10;
	Assert(norm_query_buflen > 0);

	/* Allocate result buffer */
	norm_query = palloc(norm_query_buflen + 1);

	if (jstate->clocations_count > 1)
		qsort(jstate->clocations, jstate->clocations_count,
			  sizeof(LocationLen), comp_location);

	for (i = 0; i < jstate->clocations_count; i++)
	{
		int			off,		/* Offset from start for cur tok */
					tok_len;	/* Length (in bytes) of that tok */

		off = jstate->clocations[i].location;
		/* Adjust recorded location if we're dealing with partial string */
		off -= query_loc;

		tok_len = jstate->clocations[i].length;

		if (tok_len < 0)
			continue;			/* ignore any duplicates */

		/* Copy next chunk (what precedes the next constant) */
		len_to_wrt = off - sub_query_loc;

		Assert(len_to_wrt >= 0);
		memcpy(norm_query + n_quer_loc, sub_query + sub_query_loc, len_to_wrt);
		n_quer_loc += len_to_wrt;

		/* And insert a param symbol in place of the constant token */
		n_quer_loc += sprintf(norm_query + n_quer_loc, "$%d",
							  i + 1 + jstate->highest_extern_param_id);

		if (extract_parameters)
		{
			int			bytes_written = append_str_to_parameters_buffer(sub_query + off, tok_len, true);

			if (bytes_written == 0)
				/* Nothing was written, the parameter was fully truncated */
				span->num_truncated_parameters++;
			else
				/* We have at least a partial parameter */
				span->num_parameters++;
		}

		sub_query_loc = off + tok_len;
	}

	/*
	 * We've copied up until the last ignorable constant.  Copy over the
	 * remaining bytes of the original query string.
	 */
	len_to_wrt = query_len - sub_query_loc;

	Assert(len_to_wrt >= 0);
	memcpy(norm_query + n_quer_loc, sub_query + sub_query_loc, len_to_wrt);
	n_quer_loc += len_to_wrt;

	Assert(n_quer_loc <= norm_query_buflen);
	norm_query[n_quer_loc] = '\0';

	*query_len_p = n_quer_loc;
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
