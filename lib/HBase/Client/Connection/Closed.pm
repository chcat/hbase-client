package HBase::Client::Connection::Closed;

use v5.14;
use warnings;

use parent 'HBase::Client::Connection::Disconnected';

sub connect { die "Connection closed"; }

sub write { die "Connection closed"; }

sub close {}

1;