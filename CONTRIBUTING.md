# Contributing

First of all, thanks for contributing! You can help pg_tracing in different ways:

- Open an [issue](https://github.com/DataDog/pg_tracing/issues) with suggestions for improvements, potential bugs, etc...
- Fork this repository and submit a pull request

This document provides some basic guidelines for contributing to this repository.

## Pull Requests

Have you fixed a bug or written a new check and want to share it? Many thanks!

In order to ease/speed up our review, here are some items you can check/improve
when submitting your PR:

* Have a proper commit history (we advise you to rebase if needed).
* Write tests for the code you wrote.
* Make sure that all tests pass locally.

Your pull request must pass all CI tests before we will merge it.

## Build With Debug Symbols

To build and install pg_tracing with debug symbols, use `PG_CFLAGS` env to pass the debug flags:

```bash
PG_CFLAGS="-g" make install
```

## Indentation

Code should be indented with pgindent. You need to install pg_bsd_indent first:

```
git clone https://git.postgresql.org/git/pg_bsd_indent.git
cd pg_bsd_indent
make install
```

With [pgindent](https://github.com/postgres/postgres/blob/master/src/tools/pgindent/pgindent) needs to be in your `$PATH`. Once this is done, you will be able to indent with:

```
make pgindent
```

## Tests

### Run Tests Locally

You can run checks on a local PostgreSQL version available in the `PATH` with:

```bash
make regresscheck
```

This will create a temporary instance using `pg_regress`. On failure, the diffs will be available in the `regression.diffs`.

### Run Tests In Docker

You can build a docker image with pg_tracing for a specific version.

```bash
make PG_VERSION=16 build-test-image
```

Once the docker image is built, you can launch it with:

```bash
docker run --rm --name pg_tracing_test -ti pg_tracing_test:pg16 bash
```

To launch test in the docker image:

```bash
make PG_VERSION=16 run-test
```

### Update Expected Outputs

If you have added additional tests, the expected outputs needs to be updated. If the outputs match your expectations, you can simply copy them:

```bash
cp results/*out expected/
```
