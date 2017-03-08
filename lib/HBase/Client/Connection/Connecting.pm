package HBase::Client::Connection::Connecting;

use v5.14;
use warnings;

use parent 'HBase::Client::Connection::State';

use HBase::Client::Sync;

sub enter {

    my ($self, $callback, %args) = @_;

    $self->{callback} = $callback;

    my $connection = $self->connection;

    if (my $error = $connection->_open_socket) {

        $connection->_disconnected;

        return callback $callback, $error;

    }

    $connection->_watch_can_write( $args{timeout} );

    return;

}

sub disconnect {

    my ($self, @args) = @_;

    return $self->connection->_disconnected( @args );

}

sub can_write {

    my ($self) = @_;

    $self->connection->_connected;

    call( $self->{callback} );

    return; # stop watching for can-write

}

sub can_write_timeout {

    my ($self) = @_;

    $self->connection->_disconnected;

    call( $self->{callback}, 'timeout' );

    return;
}

1;