# contrib/pg_tracing/Makefile

# Supported PostgreSQL versions:
PG_VERSIONS = 16

# Default version:
PG_VERSION ?= $(lastword $(PG_VERSIONS))

MODULE_big = pg_tracing
EXTENSION = pg_tracing
DATA = pg_tracing--1.0.sql
PGFILEDESC = "pg_tracing - Distributed tracing for postgres"
OBJS = \
	$(WIN32RES) \
	src/pg_tracing.o \
	src/pg_tracing_query_process.o \
	src/pg_tracing_span.o

REGRESSCHECKS = utility \
				select \
				extended \
				insert \
				trigger \
				sample \
				subxact \
				full_buffer \
				nested \
				wal \
				cleanup

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

LOCAL_PG_VERSION := $(shell $(PG_CONFIG) --version | sed "s/^[^ ]* \([0-9]*\).*$$/\1/" 2>/dev/null)
PG_REGRESS_ARGS=--no-locale --encoding=UTF8 --temp-config pg_tracing.conf
PG_CFLAGS := $(PG_CFLAGS) -DPG_VERSION_MAJOR=$(LOCAL_PG_VERSION)
include $(PGXS)

regress:
	$(pg_regress_check) \
        $(PG_REGRESS_ARGS) \
        $(REGRESSCHECKS)

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
		bash -c "make regress || cat /usr/src/pg_tracing/regression.diffs"
