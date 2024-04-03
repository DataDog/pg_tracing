# Supported PostgreSQL versions:
PG_VERSIONS = 16

# Default version:
PG_VERSION ?= $(lastword $(PG_VERSIONS))

MODULE_big = pg_tracing
EXTENSION  = pg_tracing
DATA       = pg_tracing--0.1.0.sql
PGFILEDESC = "pg_tracing - Distributed Tracing for PostgreSQL"
PG_CONFIG  = pg_config
OBJS = \
	$(WIN32RES) \
	src/pg_tracing.o \
	src/pg_tracing_query_process.o \
	src/pg_tracing_parallel.o \
	src/pg_tracing_planstate.o \
	src/pg_tracing_explain.o \
	src/pg_tracing_span.o

REGRESSCHECKS = setup utility select extended insert trigger sample \
				planstate planstate_bitmap planstate_hash \
				parallel subxact full_buffer \
				nested wal cleanup
REGRESSCHECKS_OPTS = --no-locale --encoding=UTF8 --temp-config pg_tracing.conf

PGXS := $(shell $(PG_CONFIG) --pgxs)

include $(PGXS)

regresscheck_noinstall:
	$(pg_regress_check) \
        $(REGRESSCHECKS_OPTS) \
        $(REGRESSCHECKS)

regresscheck: install regresscheck_noinstall

typedefs.list:
	wget -q -O typedefs.list https://buildfarm.postgresql.org/cgi-bin/typedefs.pl

.PHONY: pgindent
pgindent: typedefs.list
	pgindent --typedefs=typedefs.list \
	src/*.c \
	src/*.h

# DOCKER BUILDS
TEST_CONTAINER_NAME = pg_tracing_test
BUILD_TEST_TARGETS  = $(patsubst %,build-test-pg%,$(PG_VERSIONS))

.PHONY: build-test-image
build-test-image: build-test-pg$(PG_VERSION) ;

.PHONY: $(BUILD_TEST_TARGETS)
$(BUILD_TEST_TARGETS):
	docker build \
	  --build-arg PG_VERSION=$(PG_VERSION)	 \
	  -t $(TEST_CONTAINER_NAME):$(subst build-test-,,$@) .

.PHONY: run-test
run-test: build-test-pg$(PG_VERSION)
	docker run					                \
		--name $(TEST_CONTAINER_NAME) --rm		\
		$(TEST_CONTAINER_NAME):pg$(PG_VERSION)	\
		bash -c "make regresscheck_noinstall || cat /usr/src/pg_tracing/regression.diffs"
