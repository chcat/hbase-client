package HBase::Client::Connection::Disconnected;

use v5.14;
use warnings;

use parent 'HBase::Client::Connection::State';

use HBase::Client::Sync;

sub enter {

    my ($self) = @_;

    $self->connection->_close_socket;

    return;

}

sub connect {

    my ($self, @args) = @_;

    $self->connection->_connecting( @args );

    return;
}

1;