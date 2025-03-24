FROM ubuntu:jammy AS base

ARG PG_VERSION
# Install packages
RUN apt update
RUN DEBIAN_FRONTEND=noninteractive apt install -y --no-install-recommends \
    ca-certificates curl gnupg make gcc ruby libcurl4-gnutls-dev \
	&& rm -rf /var/lib/apt/lists/*

# Install PostgreSQL
RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt jammy-pgdg main ${PG_VERSION}" > /etc/apt/sources.list.d/pgdg.list
RUN apt update \
    && DEBIAN_FRONTEND=noninteractive apt install -y --no-install-recommends \
     postgresql-server-dev-${PG_VERSION} && rm -rf /var/lib/apt/lists/*

# Install FPM
RUN gem install fpm

# Build pg_tracing for a given --build-arg PG_VERSION target version of
# PostgreSQL.
FROM base AS build

ARG PG_VERSION
ARG EXT_VERSION
ENV LIB_DIR usr/lib/postgresql/${PG_VERSION}/lib/
ENV SHARE_DIR usr/share/postgresql/${PG_VERSION}/extension
USER postgres

WORKDIR /usr/src/pg_tracing

RUN mkdir src

# Copy sources and compile
COPY --chown=postgres common.mk ./
COPY --chown=postgres Makefile ./
COPY --chown=postgres Makefile.dist ./
COPY --chown=postgres META.json.in ./
COPY --chown=postgres *.sql ./
COPY --chown=postgres pg_tracing.control ./
COPY --chown=postgres ./src/*.c ./src
COPY --chown=postgres ./src/*.h ./src
COPY --chown=postgres pg_tracing.conf ./pg_tracing.conf

# Compile
RUN make -s

# Prepare package directories
RUN mkdir -p ${LIB_DIR}
RUN mkdir -p ${SHARE_DIR}

# Copy package files
RUN cp pg_tracing.so ${LIB_DIR}
RUN cp *.sql ${SHARE_DIR}
RUN cp pg_tracing.control ${SHARE_DIR}

# Build package
RUN fpm \
  -n postgresql-${PG_VERSION}-pg-tracing -v ${EXT_VERSION} -t deb \
  -m "<anthonin.bonnefoy@datadoghq.com>" --url "https://www.datadoghq.com/" \
  --description "Distributed Tracing for PostgreSQL" \
  --vendor "datadoghq" --license="MIT" \
  -s dir usr
