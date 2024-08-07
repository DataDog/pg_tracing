use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');

$node->init;
$node->append_conf(
	'postgresql.conf',
	qq{shared_preload_libraries = 'pg_tracing'
    pg_tracing.otel_naptime = 1000
    pg_tracing.otel_endpoint = 'http://127.0.100.100:5555'
    log_min_messages = info
});

$node->start;

# setup
$node->safe_psql("postgres",
		"CREATE EXTENSION pg_tracing;");

# Create one trace
$node->safe_psql("postgres", "/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/SELECT 1");

ok( $node->poll_query_until('postgres', "SELECT otel_failures >= 1 FROM pg_tracing_info;"),
    "Otel failures should be reported");

my $result =
  $node->safe_psql('postgres', "SELECT count(*) FROM pg_tracing_peek_spans;");
is($result, qq(0), "Query's spans should have been consumed as they were copied");

# Create a second trace
$node->safe_psql("postgres", "/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/SELECT 1");

# Wait for more failures
ok( $node->poll_query_until('postgres', "SELECT otel_failures >= 3 FROM pg_tracing_info;"),
    "Otel failures should be reported");

$result =
  $node->safe_psql('postgres', "SELECT count(*) FROM pg_tracing_peek_spans;");
is($result, qq(4), "Query's spans should still be present as we still attempt to send the previous payload");

# Cleanup
$node->stop;

done_testing();
