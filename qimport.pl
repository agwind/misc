#!/usr/bin/perl

use strict;
use warnings;
use 5.10.0;

use IO::Handle;

use JSON qw();
use MongoDB;
use Data::Dumper;
use Sereal::Decoder;
use qpid;
use Time::HiRes qw/gettimeofday tv_interval usleep/;

my $url = '127.0.0.1';
my $address = 'q0';
my $forever = 0;
my $arg_timeout = 5;
my $connectionOptions = {};

my $json = JSON->new();
my $decoder = Sereal::Decoder->new();
my $connection = new qpid::messaging::Connection( $url, $connectionOptions );
$connection->open();
my $session = $connection->create_session();
my $receiver = $session->create_receiver($address);
#my $message = new qpid::messaging::Message();

sub getTimeout {
    # returns either the named duration FOREVER if the
    # forever cmdline argument was used, otherwise creates
    # a new Duration of the specified length
    return ($forever)
      ? qpid::messaging::Duration::FOREVER
      : new qpid::messaging::Duration( $arg_timeout * 1000 );
}

my $exit = 0;

$SIG{HUP} = sub {
	$exit = 1;
};

my $debug = 0;
my $doc;

#findmaster doesn't work
my $client = MongoDB::MongoClient->new( host => 'mongodb://mongodb2,mongodb3,mongodb1', w => 'majority', j => 1, find_master => 1, query_timeout => 5000 );
my $db = $client->get_database( 'wikilinks' );
my $coll_links = $db->get_collection( 'links' );
my %count = (
	skip => 0,
	insert => 0,
	insert_fail => 0,
);



my $timeout = getTimeout();
my $t0;
my $start_time;
my $first = 0;
my $ndequeued = 0;

while (1 && !$exit) {
	my $message;
	eval { $message = $receiver->fetch($timeout); };
	my $error = $@;
	if ($error) {
		if ($error =~ /no message to fetch/i) {
			next;
		} else {
			print STDERR "$error\n";
			last;
		}
	}
	if ($message) {
		if (!$first) {
			$first = 1;
			$t0 = [gettimeofday];
			$start_time = $t0;
		}
		$ndequeued++;
		$decoder->decode($message->get_content, $doc);
		insert($doc);	
		$session->acknowledge();
	}
}
$session->sync();
$connection->close();
my $elapsed = tv_interval ( $t0 );

my $total_time = tv_interval ( $start_time );
my $jcount = $json->encode(\%count);
say "$jcount";
say "Time: $total_time";

sub insert {
	my $doc = shift;
	printStats() if (($count{skip} + $count{insert}) % 1000 == 0);
	if ($coll_links->find_one({_id=>$doc->{_id}})) {
		if($debug) { say "Skip"; }
		$count{skip}++;
		return;
	}
	if (!defined($doc->{_id})) { return; }
	my $insert_count = 0;
	do {
		$insert_count++;
		eval {
			$coll_links->insert($doc, {safe => 1});
		};
		my $fail = $@;
		if ($fail) {
			if($debug) { say "Fail: $fail"; }
			say "Fail: $fail";
			$count{insert_fail}++;
		}
	} while ($@ and ($insert_count < 100));
	if($debug) { say "Insert"; }
	$count{insert}++;
}

sub printStats {
	my $elapsed = tv_interval ( $t0 );
	printf('Docs inserted [%d]. Docs skipped [%d]. Insertion failures [%d].  Interval time [%2.3f]s' . "\n", $count{insert}, $count{skip}, $count{insert_fail},$elapsed);
	$t0 = [gettimeofday];
}
