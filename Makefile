# Supported PostgreSQL versions:
PG_VERSIONS = 15 16 17 18

MODULE_big = pg_tracing
EXTENSION  = pg_tracing
DATA       = pg_tracing--0.1.0.sql
PGFILEDESC = "pg_tracing - Distributed Tracing for PostgreSQL"
PG_CONFIG  = pg_config
# TODO: Make this optional
SHLIB_LINK = -lcurl
OBJS = \
	$(WIN32RES) \
	src/pg_tracing.o \
	src/pg_tracing_active_spans.o \
	src/pg_tracing_explain.o \
	src/pg_tracing_json.o \
	src/pg_tracing_operation_hash.o \
	src/pg_tracing_otel.o \
	src/pg_tracing_parallel.o \
	src/pg_tracing_planstate.o \
	src/pg_tracing_query_process.o \
	src/pg_tracing_span.o \
	src/pg_tracing_sql_functions.o \
	src/pg_tracing_strinfo.o \
	src/version_compat.o

# Default version:
PG_VERSION ?= $(shell $(PG_CONFIG) --version | cut -d' ' -f2 | cut -d'.' -f1 | tr -d 'devel')

REGRESSCHECKS = setup utility select parameters insert trigger cursor json transaction planstate_projectset

REGRESSCHECKS_OPTS = --no-locale --encoding=UTF8 --temp-config pg_tracing.conf

# \bind is only available starting PG 16
ifeq ($(shell test $(PG_VERSION) -ge 16; echo $$?),0)
REGRESSCHECKS += extended parameters_extended
# expecteddir is not supported by PG15, expected at the project root will only be used for PG 15 expected outputs
REGRESSCHECKS_OPTS += --expecteddir=regress/$(PG_VERSION)
endif

# infinity interval is only available starting PG 17
ifeq ($(shell test $(PG_VERSION) -ge 17; echo $$?),0)
REGRESSCHECKS += nested_17
endif

# PG 18 contains additional psql metacommand to test extended protocol
ifeq ($(shell test $(PG_VERSION) -ge 18; echo $$?),0)
REGRESSCHECKS += psql_extended_18 psql_extended_tx_18
endif

REGRESSCHECKS += sample planstate planstate_bitmap planstate_hash \
				 planstate_subplans planstate_union \
				 parallel subxact full_buffer \
				 guc nested wal cleanup

PGXS := $(shell $(PG_CONFIG) --pgxs)

TAP_TESTS = 1

include $(PGXS)

regresscheck_noinstall:
	$(pg_regress_check) $(REGRESSCHECKS_OPTS) $(REGRESSCHECKS) || \
	(cat regression.diffs && exit 1)

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
	  $(DOCKER_BUILD_OPTS) \
	  --build-arg PG_VERSION=$(PG_VERSION)	 \
	  -t $(TEST_CONTAINER_NAME):$(subst build-test-,,$@) .

.PHONY: run-test
run-test: build-test-pg$(PG_VERSION)
	docker run					                \
		--name $(TEST_CONTAINER_NAME) --rm		\
		$(TEST_CONTAINER_NAME):pg$(PG_VERSION)	\
		bash -c "make regresscheck_noinstall && make installcheck"

.PHONY: update-regress-output
update-regress-output: build-test-pg$(PG_VERSION)
	docker run					                \
		--name $(TEST_CONTAINER_NAME) --rm		\
		-v./results:/usr/src/pg_tracing/results	\
		$(TEST_CONTAINER_NAME):pg$(PG_VERSION)	\
		bash -c "make regresscheck_noinstall || true"
	@if [ $(PG_VERSION) = "15" ]; then \
		cp results/*.out expected/;	\
	else \
		cp results/*.out regress/$(PG_VERSION)/expected; \
	fi

.PHONY: update-regress-output-local
update-regress-output-local: regresscheck
	@if [ $(PG_VERSION) = "15" ]; then \
		cp results/*.out expected/;	\
	else \
		cp results/*.out regress/$(PG_VERSION)/expected; \
	fi
