ARG PGVERSION=16

FROM ubuntu:focal as base

ARG PGVERSION

# Install packages
RUN apt update
RUN DEBIAN_FRONTEND=noninteractive apt install -y --no-install-recommends \
    build-essential ca-certificates curl gnupg git flex bison iproute2 \
	libcurl4-gnutls-dev libicu-dev libncurses-dev libxml2-dev zlib1g-dev libedit-dev \
    libkrb5-dev liblz4-dev libncurses6 libpam-dev libreadline-dev libselinux1-dev \
    libssl-dev libxslt1-dev libzstd-dev uuid-dev make autoconf openssl sudo \
    psutils psmisc htop less postgresql-common \
	&& rm -rf /var/lib/apt/lists/*

RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt focal-pgdg main ${PGVERSION}" > /etc/apt/sources.list.d/pgdg.list

# Install PostgreSQL
RUN apt update \
    && DEBIAN_FRONTEND=noninteractive apt install -y --no-install-recommends \
     postgresql-server-dev-${PGVERSION} \
     postgresql-${PGVERSION} \
    && rm -rf /var/lib/apt/lists/*

RUN adduser postgres sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

#
# Build pg_tracing for a given --build-arg PGVERSION target version of
# PostgreSQL.
#
FROM base as build

ARG PGVERSION
ENV PG_CONFIG /usr/lib/postgresql/${PGVERSION}/bin/pg_config
USER postgres

WORKDIR /usr/src/pg_tracing

COPY --chown=postgres Makefile ./
COPY --chown=postgres pg_tracing--1.0.sql pg_tracing.control ./
COPY --chown=postgres ./src/ ./src

# Tests
COPY --chown=postgres ./sql/ ./sql
COPY --chown=postgres ./tests/ ./tests

RUN make -s clean
RUN sudo make -s install -j8

#
# Given the build image above, we can now run our test suite targetting a
# given version of Postgres.
#
FROM build as test

ARG PGVERSION
USER postgres
ENV PATH /usr/lib/postgresql/${PGVERSION}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
