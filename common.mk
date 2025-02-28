# Supported PostgreSQL versions:
PG_VERSIONS = 14 15 16 17 18

EXTENSION  = pg_tracing
PG_CONFIG  = pg_config

# Get postgres version
PG_CONFIG_EXISTS := $(shell command -v $(PG_CONFIG) 2> /dev/null)
ifdef PG_CONFIG_EXISTS
# Default to pg_config's advertised version
PG_VERSION ?= $(shell $(PG_CONFIG) --version | cut -d' ' -f2 | cut -d'.' -f1 | tr -d 'devel')
else
# pg_config is not present, let's assume we are packaging and use the latest PG version
PG_VERSION ?= $(lastword $(PG_VERSIONS))
endif

# Get extension version
GIT_EXISTS := $(shell command -v git 2> /dev/null)
ifdef GIT_EXISTS
EXT_VERSION = $(shell git describe --tags | sed 's/^v//')
else
EXT_VERSION = "0.1.0"
endif

TMP_DIR=$(PWD)/tmp

EXTRA_CLEAN = META.json $(TMP_DIR)
