/*-------------------------------------------------------------------------
 *
 * src/version_compat.h
 *    Compatibility macros and functions for writing code agnostic to PostgreSQL versions
 *
 *-------------------------------------------------------------------------
 */

#ifndef VERSION_COMPAT_H
#define VERSION_COMPAT_H

#include "postgres.h"

#if (PG_VERSION_NUM < 150000)

#include "pg_prng.h"

typedef double Cardinality;

/*
 * Thin wrappers that convert strings to exactly 64-bit integers, matching our
 * definition of int64.  (For the naming, compare that POSIX has
 * strtoimax()/strtoumax() which return intmax_t/uintmax_t.)
 */
#if SIZEOF_LONG == 8
#define strtoi64(str, endptr, base) ((int64) strtol(str, endptr, base))
#define strtou64(str, endptr, base) ((uint64) strtoul(str, endptr, base))
#elif SIZEOF_LONG_LONG == 8
#define strtoi64(str, endptr, base) ((int64) strtoll(str, endptr, base))
#define strtou64(str, endptr, base) ((uint64) strtoull(str, endptr, base))
#else
#error "cannot find integer type of the same size as int64_t"
#endif

#endif

#if (PG_VERSION_NUM < 160000)

#include "utils/queryjumble.h"
#define NS_PER_S	INT64CONST(1000000000)

/* We need to explicitly declare _PG_init */
void
			_PG_init(void);

/* Functions that were introduced with PG 16 */
extern void *repalloc0(void *pointer, Size oldsize, Size size);
extern void *guc_malloc(int elevel, size_t size);

#else
#include "nodes/queryjumble.h"
#endif

#if (PG_VERSION_NUM < 170000)

#include "storage/backendid.h"
/*
 * Compatibility with changes introduced in
 * https://www.postgresql.org/message-id/8171f1aa-496f-46a6-afc3-c46fe7a9b407@iki.fi
 */

#define INVALID_PROC_NUMBER InvalidBackendId
#define MyProcNumber MyBackendId
#define ProcNumber BackendId
#define ParallelLeaderProcNumber ParallelLeaderBackendId

#endif

#if (PG_VERSION_NUM < 180000)

#define UINT64_HEX_PADDED_FORMAT  "%016" INT64_MODIFIER "x"
#define EXECUTOR_RUN(...) \
		if (prev_ExecutorRun) \
			prev_ExecutorRun(queryDesc, direction, count, execute_once); \
		else \
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
#else

#define UINT64_HEX_PADDED_FORMAT  "%016" PRIx64
#define EXECUTOR_RUN(...) \
		if (prev_ExecutorRun) \
			prev_ExecutorRun(queryDesc, direction, count); \
		else \
			standard_ExecutorRun(queryDesc, direction, count);
#endif

#endif							/* VERSION_COMPAT_H */
