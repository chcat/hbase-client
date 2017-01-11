package HBase::Client::Connection::Opened;

use v5.14;
use warnings;

use parent HBase::Client::Connection::Disconnected;

sub new {

    return shift->SUPER::_new( @_ );

}

1;