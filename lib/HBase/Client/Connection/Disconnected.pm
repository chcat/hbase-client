package HBase::Client::Connection::Disconnected;

use v5.14;
use warnings;

use parent 'HBase::Client::Connection::State';

sub _enter {

     my ($self) = @_;

     $self->_close_socket;

}

sub connect {

    my ($self, $callback, %args) = @_;

    $self->_state( 'HBase::Client::Connection::Connecting', [ $callback, %args ] );

    return;
}

1;