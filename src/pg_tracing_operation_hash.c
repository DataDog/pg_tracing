/*-------------------------------------------------------------------------
 *
 * pg_tracing_operation_hash.c
 * 		pg_tracing plan explain functions.
 *
 * IDENTIFICATION
 *	  src/pg_tracing_operation_hash.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "utils/hsearch.h"
#include "storage/shmem.h"
#include "pg_tracing.h"

static HTAB *operation_name_hash = NULL;

typedef struct operationKey
{
	uint64		query_id;
	SpanType	span_type;
}			operationKey;

typedef struct operationEntry
{
	operationKey key;			/* hash key of entry - MUST BE FIRST */
	Size		query_offset;	/* query text offset in external file */
}			operationEntry;

/*
 * Initialize operation hash in shared memory
 */
void
init_operation_hash(void)
{
	HASHCTL		info;

	info.keysize = sizeof(operationKey);
	info.entrysize = sizeof(operationEntry);
	operation_name_hash = ShmemInitHash("pg_tracing hash",
										100, 50000, &info,
										HASH_ELEM | HASH_BLOBS);
}

/*
 * Reset content of the operation hash table
 */
void
reset_operation_hash(void)
{
	HASH_SEQ_STATUS status;
	operationEntry *hentry;

	Assert(operation_name_hash != NULL);

	/* Currently we just flush all entries; hard to be smarter ... */
	hash_seq_init(&status, operation_name_hash);

	while ((hentry = (operationEntry *) hash_seq_search(&status)) != NULL)
	{
		if (hash_search(operation_name_hash,
						&hentry->key,
						HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "hash table corrupted");
	}
}

Size
lookup_operation_name(const Span * span, const char *txt)
{
	bool		found;
	Size		offset;
	operationKey key;
	operationEntry *entry;

	key.query_id = span->query_id;
	key.span_type = span->type;
	Assert(span->query_id != 0);

	if (operation_name_hash == NULL)
	{
		HASHCTL		info;

		info.keysize = sizeof(operationKey);
		info.entrysize = sizeof(operationEntry);
		operation_name_hash = ShmemInitHash("pg_tracing operation name hash", 100,
											10000, &info, HASH_ELEM | HASH_BLOBS);
	}
	entry = (operationEntry *) hash_search(operation_name_hash, &key, HASH_ENTER, &found);

	if (found)
	{
		return entry->query_offset;
	}

	offset = pg_tracing_shared_state->extent;
	append_str_to_shared_str(txt, strlen(txt) + 1);
	/* Update hash's entry */
	entry->query_offset = offset;

	return offset;
}
