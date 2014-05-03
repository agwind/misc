#!/usr/bin/perl

use strict;
use warnings;
use 5.10.0;

use qpid;
use qpid::messaging;
use Data::Dumper;
use Sereal::Encoder;
use Time::HiRes qw/gettimeofday tv_interval/;

my $enc = Sereal::Encoder->new();

my $debug = 0;
my $url = '127.0.0.1';
my $address = 'q0';
my $connectionOptions = {};
my %doc;
my %wurl;
my $count=0;

my $filename = $ARGV[0];

if ( !-f $filename ) {
	die "./$0 <file>\n";
}

open( my $fh, '<', $filename ) or die "Can't open file: $filename";

my $connection = qpid::messaging::Connection->new( $url, $connectionOptions );
$connection->open();
my $session = $connection->create_session();
my $sender  = $session->create_sender($address);
my $message = qpid::messaging::Message->new();

my $start_time = [gettimeofday];

while ( my $line = <$fh> ) {
	chomp($line);
	#print "L: $line\n";
	if ( $line =~ /^URL\t(?<url>.*)/ ) {
		my $url = $+{url};
		$doc{_id} = $url;
	}
	if ( $line =~ /^MENTION\t(?<title>.*)\t(?<offset>\d+)\t(?<wurl>.*)/ ) {
		push @{ $wurl{ $+{wurl} }->{ $+{title} } }, $+{offset};
	}
	if ( $line =~ /^TOKEN\t(?<token>.*)\t(?<offset>\d+)/ ) {
		push @{$doc{tokens}}, { token => $+{token}, offset => $+{offset} };
	}
	if ( $line eq '' ) {
		$line = <$fh>; # read next new line
		if (!defined($doc{_id})) { next; }
		my @wdoc;
		foreach my $wiki_url (keys %wurl) {
			my %whash;
			$whash{url} = $wiki_url;
			foreach my $title (keys %{$wurl{$wiki_url}}) {
				push @{$whash{titles}}, { title => $title, positions => $wurl{$wiki_url}->{$title}};
			}
			push @wdoc, { %whash } ;
		}
		$doc{wurl} = \@wdoc;
		$message->set_content($enc->encode(\%doc));
		$message->set_content_type("application/octet-stream");
		$sender->send($message);
		$count++;
		%doc = ();
		%wurl = ();
	}
}

$session->sync();
$connection->close();
close($fh);

my $total_time = tv_interval ( $start_time );
say "Queued: $count";
say "Time: $total_time";
