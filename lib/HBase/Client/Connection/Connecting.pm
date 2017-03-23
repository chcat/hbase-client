package HBase::Client::Connection::Connecting;

use v5.14;
use warnings;

use parent 'HBase::Client::Connection::State';

sub enter {

    my ($self, $callback, %args) = @_;

    my $connection = $self->connection;

    if (my $error = $connection->_open_socket) {

        $connection->_disconnected( $error, $callback );

    } else {

        $self->{callback} = $callback;

        $connection->_watch_can_write( $args{timeout} );

    }

    return;

}

sub disconnect {

    my ($self, $reason) = @_;

    $self->connection->_disconnected( $reason, $self->{callback} );

    return;

}

sub can_write {

    my ($self) = @_;

    my $connection = $self->connection;

    $connection->_unwatch_can_write;

    $connection->_connected( $self->{callback} );

    return;

}

sub can_write_timeout {

    my ($self) = @_;

    $self->connection->_disconnected( 'Connection timeout', $self->{callback} );

    return;
}

1;