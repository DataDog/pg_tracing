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


#endif							/* VERSION_COMPAT_H */
