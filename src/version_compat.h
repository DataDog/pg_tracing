/*-------------------------------------------------------------------------
 *
 * src/version_compat.h
 *    Compatibility macros for writing code agnostic to PostgreSQL versions
 *
 *-------------------------------------------------------------------------
 */

#ifndef VERSION_COMPAT_H
#define VERSION_COMPAT_H

#include "postgres.h"

#if PG_VERSION_NUM < 170000

/*
 * Compatibility with changes introduced in
 * https://www.postgresql.org/message-id/8171f1aa-496f-46a6-afc3-c46fe7a9b407@iki.fi
 */

#define INVALID_PROC_NUMBER InvalidBackendId
#define MyProcNumber MyBackendId
#define ProcNumber BackendId
#define ParallelLeaderProcNumber ParallelLeaderBackendId

#endif

#endif   /* VERSION_COMPAT_H */
