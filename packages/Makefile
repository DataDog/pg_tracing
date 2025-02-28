include ../common.mk

BUILD_CONTAINER_NAME = pg_tracing_build
DOCKER_BUILD_OPTS 	 = --rm --build-arg PG_VERSION=$(PG_VERSION) --build-arg EXT_VERSION=$(EXT_VERSION)

DEB_PACKAGE_X86_64=$(EXTENSION)_$(PG_VERSION)_$(EXT_VERSION)_amd64.deb
DEB_PACKAGE_ARM64=$(EXTENSION)_$(PG_VERSION)_$(EXT_VERSION)_arm64.deb

IMAGE_NAME_AMD64 = $(BUILD_CONTAINER_NAME):pg_$(PG_VERSION)_amd64
IMAGE_NAME_ARM64 = $(BUILD_CONTAINER_NAME):pg_$(PG_VERSION)_arm64

.PHONY: all

all: $(TMP_DIR)/$(DEB_PACKAGE_X86_64) $(TMP_DIR)/$(DEB_PACKAGE_ARM64)

$(TMP_DIR)/$(DEB_PACKAGE_X86_64):
	docker build -t $(IMAGE_NAME_AMD64) \
		--platform linux/amd64 $(DOCKER_BUILD_OPTS) -f deb.Dockerfile ..
	docker run $(IMAGE_NAME_AMD64) \
		--platform linux/amd64 -v $(TMP_DIR):/out bash -c "cp /usr/src/pg_tracing/$(DEB_PACKAGE_X86_64) /out"

$(TMP_DIR)/$(DEB_PACKAGE_ARM64):
	docker build -t $(IMAGE_NAME_AMD64) \
		--platform linux/arm64 $(DOCKER_BUILD_OPTS) -f deb.Dockerfile ..
	docker run $(IMAGE_NAME_ARM64) \
		--platform linux/arm64 -v $(TMP_DIR):/out bash -c "cp /usr/src/pg_tracing/$(DEB_PACKAGE_ARM64) /out"
