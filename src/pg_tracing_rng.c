/*-------------------------------------------------------------------------
 *
 * pg_tracing_rng.c
 * 		pg_tracing rng functions.
 *
 * IDENTIFICATION
 *	  src/pg_tracing_rng.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_prng.h"
#include "pg_tracing.h"

static void
check_rng_seed(void)
{
#if (PG_VERSION_NUM < 150000)
	/* pid used for seeding */
	static uint64 seeded_pid = 0;

	if (MyProcPid != seeded_pid)
	{
		/* We are in a child process, reseed prng with our new pid */
		uint64		rseed;

		seeded_pid = MyProcPid;
		rseed = ((uint64) MyProcPid) ^
			((uint64) MyStartTimestamp << 12) ^
			((uint64) MyStartTimestamp >> 20);
		pg_prng_seed(&pg_global_prng_state, rseed);
	}
#endif
}

double
generate_rnd_double(void)
{
	check_rng_seed();
	return pg_prng_double(&pg_global_prng_state);
}

uint64
generate_rnd_uint64(void)
{
	check_rng_seed();
	return pg_prng_uint64(&pg_global_prng_state);
}
