/*-------------------------------------------------------------------------
 *
 * Pseudo-Random Number Generator
 *
 * Copyright (c) 2021-2025, PostgreSQL Global Development Group
 *
 * src/include/common/pg_prng.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PRNG_H
#define PG_PRNG_H

/*
 * State vector for PRNG generation.  Callers should treat this as an
 * opaque typedef, but we expose its definition to allow it to be
 * embedded in other structs.
 */
typedef struct pg_prng_state
{
	uint64		s0,
				s1;
} pg_prng_state;

/*
 * Callers not needing local PRNG series may use this global state vector,
 * after initializing it with one of the pg_prng_...seed functions.
 */
extern PGDLLIMPORT pg_prng_state pg_global_prng_state;

extern void pg_prng_seed(pg_prng_state *state, uint64 seed);
extern bool pg_prng_seed_check(pg_prng_state *state);

extern uint64 pg_prng_uint64(pg_prng_state *state);
extern double pg_prng_double(pg_prng_state *state);

#endif							/* PG_PRNG_H */
