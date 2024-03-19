/*-------------------------------------------------------------------------
 *
 * pg_tracing_query_process.c
 * 		pg_tracing query processing functions.
 *
 * IDENTIFICATION
 *	  contrib/pg_tracing/pg_tracing_explain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>
#include "parser/scanner.h"
#include "pg_tracing.h"
#include "nodes/extensible.h"

/*
 * Check if we have a comment that could store SQLComment information at the
 * start or end of the query.
 * Returns 0 if there's possible comment at the begining, 1 at the end and -1
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

	/* Check if we have comment at the begining */
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
static pgTracingTraceparent
parse_traceparent_value(const char *traceparent_str)
{
	pgTracingTraceparent traceparent;
	char	   *traceid_left;
	char	   *endptr;

	traceparent.trace_id.traceid_left = 0;
	traceparent.trace_id.traceid_right = 0;
	traceparent.sampled = 0;

	/* Check that '-' are at the expected places */
	if (traceparent_str[2] != '-' ||
		traceparent_str[35] != '-' ||
		traceparent_str[52] != '-')
		return traceparent;

	/* Parse traceparent parameters */
	errno = 0;

	traceid_left = pnstrdup(&traceparent_str[3], 16);
	traceparent.trace_id.traceid_left = strtou64(traceid_left, &endptr, 16);
	pfree(traceid_left);
	traceparent.trace_id.traceid_right = strtou64(&traceparent_str[3 + 16], &endptr, 16);

	if (endptr != traceparent_str + 35 || errno)
		return traceparent;
	traceparent.parent_id = strtou64(&traceparent_str[36], &endptr, 16);
	if (endptr != traceparent_str + 52 || errno)
		return traceparent;
	traceparent.sampled = strtol(&traceparent_str[53], &endptr, 16);
	if (endptr != traceparent_str + 55 || errno)
		/* Just to be sure, reset sampled on error */
		traceparent.sampled = 0;

	return traceparent;
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
extract_trace_context_from_query(pgTracingTraceContext * trace_context,
								 const char *query)
{
	const char *start_sqlcomment = NULL;
	const char *end_sqlcomment;

	int			possible_comment = has_possible_sql_comment(query);

	if (possible_comment == 0)
	{
		/* We have a possible comment at the begining */
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

	parse_trace_context(trace_context, start_sqlcomment,
						end_sqlcomment - start_sqlcomment);
}

/*
 * Parse trace context value.
 * We're only interested in traceparent value to get the trace id, parent id and sampled flag.
 */
void
parse_trace_context(pgTracingTraceContext * trace_context, const char *trace_context_str,
					int trace_context_len)
{
	const char *traceparent_str;
	const char *end_trace_context;

	end_trace_context = trace_context_str + trace_context_len;

	/*
	 * Locate traceparent parameter and make sure it has the expected size
	 * "traceparent" + "=" + '' -> 13 characters
	 * 00-00000000000000000000000000000009-0000000000000005-01 -> 55
	 * characters
	 */
	traceparent_str = strstr(trace_context_str, "traceparent='");
	if (traceparent_str == NULL ||
		traceparent_str > end_trace_context ||
		end_trace_context - traceparent_str < 55 + 13)
		return;

	/* Move to the start of the traceparent values */
	traceparent_str = traceparent_str + 13;
	/* And parse it */
	trace_context->traceparent = parse_traceparent_value(traceparent_str);
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
 * Parameters are put in the param_str wich will contain all parameters values
 * using the format: "$1 = 0, $2 = 'v'"
 */
const char *
normalise_query_parameters(const JumbleState *jstate, const char *query,
						   int query_loc, int *query_len_p, char **param_str,
						   int *param_len)
{
	char	   *norm_query;
	int			query_len = *query_len_p;
	int			norm_query_buflen,	/* Space allowed for norm_query */
				n_quer_loc = 0;
	LocationLen *locs;
	core_yyscan_t yyscanner;
	core_yy_extra_type yyextra;
	core_YYSTYPE yylval;
	YYLTYPE		yylloc;
	int			current_loc = 0;
	StringInfoData buf;

	initStringInfo(&buf);

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
			appendStringInfo(&buf,
							 "%s$%d = ",
							 current_loc > 0 ? ", " : "",
							 current_loc + 1);
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
				appendStringInfoChar(&buf, '-');
				tok = core_yylex(&yylval, &yylloc, yyscanner);
				if (tok == 0)
					break;		/* out of inner for-loop */
			}
			if (yylloc > 0 && yyextra.scanbuf[yylloc - 1] == ' ' && n_quer_loc > 0)
			{
				norm_query[n_quer_loc++] = ' ';
			}

			/*
			 * Append the current parameter $x in the normalised query
			 */
			n_quer_loc += sprintf(norm_query + n_quer_loc, "$%d",
								  current_loc + 1 + jstate->highest_extern_param_id);

			appendStringInfoString(&buf, yyextra.scanbuf + yylloc);

			current_loc++;
		}
		else
		{
			int			to_copy;

			if (yylloc > 0 && yyextra.scanbuf[yylloc - 1] == ' ' && n_quer_loc > 0)
			{
				norm_query[n_quer_loc++] = ' ';
			}
			to_copy = strlen(yyextra.scanbuf + yylloc);
			Assert(n_quer_loc + to_copy < norm_query_buflen + 1);
			memcpy(norm_query + n_quer_loc, yyextra.scanbuf + yylloc, to_copy);
			n_quer_loc += to_copy;
		}
	}
	scanner_finish(yyscanner);

	*query_len_p = n_quer_loc;
	norm_query[n_quer_loc] = '\0';
	*param_str = buf.data;
	*param_len = buf.len;
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

		if (yylloc > 0 && yyextra.scanbuf[yylloc - 1] == ' ' && n_quer_loc > 0)
		{
			norm_query[n_quer_loc++] = ' ';
		}
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

/*
 * Store text in the pg_tracing stat file
 */
bool
text_store_file(pgTracingSharedState * pg_tracing, const char *text, int text_len,
				Size *query_offset)
{
	Size		off;
	int			fd;

	off = pg_tracing->extent;
	pg_tracing->extent += text_len + 1;

	/*
	 * Don't allow the file to grow larger than what qtext_load_file can
	 * (theoretically) handle.  This has been seen to be reachable on 32-bit
	 * platforms.
	 */
	if (unlikely(text_len >= MaxAllocHugeSize - off))
	{
		errno = EFBIG;			/* not quite right, but it'll do */
		fd = -1;
		goto error;
	}

	/* Now write the data into the successfully-reserved part of the file */
	fd = OpenTransientFile(PG_TRACING_TEXT_FILE, O_RDWR | O_CREAT | PG_BINARY);
	if (fd < 0)
		goto error;

	if (pg_pwrite(fd, text, text_len, off) != text_len)
		goto error;
	if (pg_pwrite(fd, "\0", 1, off + text_len) != 1)
		goto error;

	CloseTransientFile(fd);

	/*
	 * Set offset once write was succesful
	 */
	*query_offset = off;

	return true;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not write file \"%s\": %m",
					PG_TRACING_TEXT_FILE)));

	if (fd >= 0)
		CloseTransientFile(fd);

	return false;
}

/*
 * Read the external query text file into a malloc'd buffer.
 *
 * Returns NULL (without throwing an error) if unable to read, eg file not
 * there or insufficient memory.
 *
 * On success, the buffer size is also returned into *buffer_size.
 *
 * This can be called without any lock on pgss->lock, but in that case the
 * caller is responsible for verifying that the result is sane.
 */
const char *
qtext_load_file(Size *buffer_size)
{
	char	   *buf;
	int			fd;
	struct stat stat;
	Size		nread;

	fd = OpenTransientFile(PG_TRACING_TEXT_FILE, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		if (errno != ENOENT)
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							PG_TRACING_TEXT_FILE)));
		return NULL;
	}

	/* Get file length */
	if (fstat(fd, &stat))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m",
						PG_TRACING_TEXT_FILE)));
		CloseTransientFile(fd);
		return NULL;
	}

	/* Allocate buffer; beware that off_t might be wider than size_t */
	if (stat.st_size <= MaxAllocHugeSize)
		buf = (char *) malloc(stat.st_size);
	else
		buf = NULL;
	if (buf == NULL)
	{
		ereport(LOG,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Could not allocate enough memory to read file \"%s\".",
						   PG_TRACING_TEXT_FILE)));
		CloseTransientFile(fd);
		return NULL;
	}

	/*
	 * OK, slurp in the file.  Windows fails if we try to read more than
	 * INT_MAX bytes at once, and other platforms might not like that either,
	 * so read a very large file in 1GB segments.
	 */
	nread = 0;
	while (nread < stat.st_size)
	{
		int			toread = Min(1024 * 1024 * 1024, stat.st_size - nread);

		/*
		 * If we get a short read and errno doesn't get set, the reason is
		 * probably that garbage collection truncated the file since we did
		 * the fstat(), so we don't log a complaint --- but we don't return
		 * the data, either, since it's most likely corrupt due to concurrent
		 * writes from garbage collection.
		 */
		errno = 0;
		if (read(fd, buf + nread, toread) != toread)
		{
			if (errno)
				ereport(LOG,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": %m",
								PG_TRACING_TEXT_FILE)));
			free(buf);
			CloseTransientFile(fd);
			return NULL;
		}
		nread += toread;
	}

	if (CloseTransientFile(fd) != 0)
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", PG_TRACING_TEXT_FILE)));

	*buffer_size = nread;
	return buf;
}
