# Supported PostgreSQL versions:
PG_VERSIONS = 14 15 16 17 18

EXTENSION  = pg_tracing

PG_CONFIG_EXISTS := $(shell command -v $(PG_CONFIG) 2> /dev/null)
ifdef PG_CONFIG_EXISTS
# Default to pg_config's advertised version
PG_VERSION ?= $(shell $(PG_CONFIG) --version | cut -d' ' -f2 | cut -d'.' -f1 | tr -d 'devel')
else
# pg_config is not present, let's assume we are packaging and use the latest PG version
PG_VERSION ?= $(lastword $(PG_VERSIONS))
endif
