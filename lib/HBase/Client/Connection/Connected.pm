package HBase::Client::Connection::Connected;

use v5.14;
use warnings;

use parent 'HBase::Client::Connection::State';

use HBase::Client::Try qw( call );

sub enter {

    my ($self) = @_;

    $self->{write_progress} = 0;

    $self->{write_queue} = [];

    $self->connection->_watch_can_read;

    return;

}

sub write {

    my ($self, $callback, $data_ref) = @_;

    my $write = {
            buffer_ref => $data_ref,
            callback   => $callback
        };

    my $write_queue = $self->{write_queue};

    push @$write_queue, $write;

    my $connection = $self->connection;

    $connection->_watch_can_write unless $connection->_watching_can_write;

    return;

}

sub disconnect {

    my ($self, $reason) = @_;

    $self->connection->_disconnected( $reason );

    my $write_queue = $self->{write_queue};

    for my $write ( @$write_queue ){

        call( $write->{callback}, $reason ? "Disconnected: $reason" : 'Disconnected' );

    }

    return;

}

sub can_write {

    my ($self) = @_;

    my $connection = $self->connection;

    my $write_queue = $self->{write_queue};

    my $write = $write_queue->[0];

    my ($buffer_ref, $callback) = @$write{ qw ( buffer_ref callback ) };

    my $write_progress = $self->{write_progress};

    my $to_write = (length $$buffer_ref) - $write_progress;

    my ($error, $written) = $connection->_socket_write( $buffer_ref, $to_write, $write_progress );

    if ($error) {

        $self->disconnect( "Write error: $error" );

        return;

    } else {

        if ($written == $to_write){

            $self->{write_progress} = 0;

            shift @$write_queue;

            call( $callback );

        } else {

            $self->{write_progress} = $write_progress + $written;

        }

    }

    $connection->_unwatch_can_write unless @$write_queue;

    return;

}

sub can_write_timeout {

    my ($self) = @_;

    $self->disconnect( 'Write timeout' );

    return;
}

sub can_read {

    my ($self)= @_;

    my $connection = $self->connection;

    my ($error, $data_ref) = $connection->_socket_read;

    if ($error) {

        $self->disconnect( "Read error: $error" );

    } else {

        $connection->_on_read( $data_ref ) if $data_ref;

    }

    return;

}

1;