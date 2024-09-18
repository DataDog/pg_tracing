use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');

{
    package TestWebServer;

    use HTTP::Server::Simple::CGI;
    use base qw(HTTP::Server::Simple::CGI);

    sub handle_request {
        print "HTTP/1.0 200 OK\r\n";
    }
}
# Start test http server that will receive spans
my $webserver_pid = TestWebServer->new(4318)->background();

$SIG{TERM} = $SIG{INT} = sub {
    kill 'INT', $webserver_pid;
    die "death by signal";
};

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
$node->safe_psql("postgres", "/*traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/SELECT 1");

# Sending spans should fail as endpoint is incorrect
ok( $node->poll_query_until('postgres', "SELECT otel_failures >= 1 FROM pg_tracing_info;"),
    "Otel failures should be reported");

# Update otel endpoint
$node->append_conf('postgresql.conf', "pg_tracing.otel_endpoint = 'http://localhost:4318'");
$node->reload;

# Sending span should eventually succeed with the new endpoint
ok( $node->poll_query_until('postgres',
        "SELECT otel_sent_spans FROM pg_tracing_info;",
        4),
    "Spans were sent to otel collector");

my $result =
  $node->safe_psql('postgres', "SELECT count(*) FROM pg_tracing_peek_spans;");
is($result, qq(0), 'All spans should have been consumed');

# Cleanup
$node->stop;
kill 'INT', $webserver_pid;

done_testing();
