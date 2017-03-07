package HBase::Client::Connection::Disconnected;

use v5.14;
use warnings;

use parent 'HBase::Client::Connection::State';

use HBase::Client::Sync;

sub enter {

    my ($self, $reason) = @_;

    my $connection = $self->connection;

    $connection->_close_socket;

    my $queue = $connection->_write_queue;

    for my $write ( @$queue ){

        call( $write->{callback}, $reason ? "Disconnected: $reason" : 'Disconnected' );

    }

    splice @$queue;

    return;

}

sub connect {

    my ($self, @args) = @_;

    $self->connection->_connecting( @args );

    return;
}

1;