package HBase::Client::Connection;

use v5.14;
use warnings;

use HBase::Client::Connection::Opened;

sub open {

    my ($class, %args)= @_;

    return HBase::Client::Connection::Opened->new( %args );

}

1;