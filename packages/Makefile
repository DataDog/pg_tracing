include ../common.mk

BUILD_CONTAINER_NAME = pg_tracing_build
DOCKER_BUILD_OPTS 	 = --rm --build-arg PG_VERSION=$(PG_VERSION) --build-arg EXT_VERSION=$(EXT_VERSION)
ARCH ?= $(shell uname -m)

# Underscore are not allowed in deb package names
DEB_PACKAGE=postgresql-$(PG_VERSION)-pg-tracing_$(EXT_VERSION)_$(ARCH).deb
IMAGE_NAME = $(BUILD_CONTAINER_NAME):pg_$(PG_VERSION)_$(ARCH)

.PHONY: all

all: $(TMP_DIR)/$(DEB_PACKAGE)

$(TMP_DIR)/$(DEB_PACKAGE):
	@mkdir -p $(TMP_DIR)
	docker build --rm --platform linux/$(ARCH) \
		$(DOCKER_BUILD_OPTS) -f deb.Dockerfile -t $(IMAGE_NAME) ..
	docker run --rm --platform linux/$(ARCH) \
		-v $(TMP_DIR):/out $(IMAGE_NAME) bash -c "cp /usr/src/pg_tracing/$(DEB_PACKAGE) /out"
