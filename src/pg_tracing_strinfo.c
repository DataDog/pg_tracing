/*-------------------------------------------------------------------------
 *
 * pg_tracing_strinfo.c
 * 		utility functions for strinfo manipulation.
 *
 * IDENTIFICATION
 *	  src/pg_tracing_strinfo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_tracing.h"

/*
 * Append a null terminated string to provided StringInfo.
 * Returns the position where str was inserted
 */
int
appendStringInfoNT(StringInfo strinfo, const char *str, int str_len)
{
	int			position = strinfo->len;

	Assert(str_len > 0);

	appendBinaryStringInfo(strinfo, str, str_len);
	appendStringInfoChar(strinfo, '\0');
	return position;
}
