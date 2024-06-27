use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');

# perl -MCPAN -e 'install -force HTTP::Server::Simple'
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
    pg_tracing.otel_endpoint = 'http://localhost:4318'
    log_min_messages = info
});

$node->start;

# setup
$node->safe_psql("postgres",
		"CREATE EXTENSION pg_tracing;");

# Create one span
$node->safe_psql("postgres", "/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/SELECT 1;\n");

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
