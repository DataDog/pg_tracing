/*-------------------------------------------------------------------------
 *
 * version_compat.c
 *    Compatibility macros and functions for writing code agnostic to PostgreSQL versions
 *
 * IDENTIFICATION
 *	  contrib/pg_tracing/version_compat.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "version_compat.h"

#if PG_VERSION_NUM < 160000

/*
 * repalloc0
 *		Adjust the size of a previously allocated chunk and zero out the added
 *		space.
 */
void *
repalloc0(void *pointer, Size oldsize, Size size)
{
	void	   *ret;

	/* catch wrong argument order */
	if (unlikely(oldsize > size))
		elog(ERROR, "invalid repalloc0 call: oldsize %zu, new size %zu",
			 oldsize, size);

	ret = repalloc(pointer, size);
	memset((char *) ret + oldsize, 0, (size - oldsize));
	return ret;
}

void *
guc_malloc(int elevel, size_t size)
{
	void	   *data;

	/* Fallback to malloc */
	data = malloc(size);

	return data;
}

#endif
