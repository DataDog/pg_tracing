FROM ubuntu:jammy AS base

ARG PG_VERSION

# Install packages
RUN apt update
RUN DEBIAN_FRONTEND=noninteractive apt install -y --no-install-recommends \
    ca-certificates curl gnupg make sudo gcc \
	&& rm -rf /var/lib/apt/lists/*

# Install PostgreSQL
RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

RUN echo "deb http://apt.postgresql.org/pub/repos/apt jammy-pgdg main ${PG_VERSION}" > /etc/apt/sources.list.d/pgdg.list
RUN if [ "${PG_VERSION}" = "18" ] ; then \
    echo "deb http://apt.postgresql.org/pub/repos/apt jammy-pgdg-snapshot main ${PG_VERSION}" >> /etc/apt/sources.list.d/pgdg.list; \
    echo "Package: *\nPin: release a=jammy-pgdg-snapshot\nPin-Priority: 900" > /etc/apt/preferences.d/99pgdg; \
fi

# Install PostgreSQL
RUN apt update \
    && DEBIAN_FRONTEND=noninteractive apt install -y --no-install-recommends \
     libcurl4-gnutls-dev libhttp-server-simple-perl libipc-run-perl \
     postgresql-server-dev-${PG_VERSION} \
     postgresql-${PG_VERSION} \
    && rm -rf /var/lib/apt/lists/*

RUN adduser postgres sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

#
# Build pg_tracing for a given --build-arg PG_VERSION target version of
# PostgreSQL.
#
FROM base AS build

ARG PG_VERSION
USER postgres

WORKDIR /usr/src/pg_tracing

RUN mkdir src

# Copy sources and compile
COPY --chown=postgres common.mk ./
COPY --chown=postgres Makefile ./
COPY --chown=postgres typedefs.list ./
COPY --chown=postgres *.sql ./
COPY --chown=postgres pg_tracing.control ./
COPY --chown=postgres ./src/*.c ./src
COPY --chown=postgres ./src/*.h ./src
COPY --chown=postgres pg_tracing.conf ./pg_tracing.conf

# Copy test files
COPY --chown=postgres ./sql/ ./sql
COPY --chown=postgres ./t/ ./t
COPY --chown=postgres ./regress/ ./regress
COPY --chown=postgres ./expected/ ./expected

# Create empty results for mount bind
# Used to copy test results and update expected output
RUN mkdir results
RUN chown postgres:postgres results

# Compile
RUN make -s
# And install
RUN sudo make -s install -j8

# Create test cluster
RUN /usr/lib/postgresql/${PG_VERSION}/bin/initdb -D /usr/src/pg_tracing/test_db/ -Atrust
COPY --chown=postgres tests/postgresql.conf /usr/src/pg_tracing/test_db/

#
# Given the build image above, we can now run our test suite targetting a
# given version of Postgres.
#
FROM build AS test

ARG PG_VERSION
USER postgres
ENV PATH=/usr/lib/postgresql/${PG_VERSION}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
