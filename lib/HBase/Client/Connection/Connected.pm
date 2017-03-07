package HBase::Client::Connection::Connected;

use v5.14;
use warnings;

use parent 'HBase::Client::Connection::State';

use HBase::Client::Sync;

sub enter {

    my ($self) = @_;

    $self->{write_progress} = 0;

    my $connection = $self->connection;

    $connection->_watch_can_read;

    $connection->_watch_can_write if @{$connection->_write_queue};

    return;

}

sub write {

    my ($self, @args) = @_;

    $self->SUPER::write( @args );

    my $connection = $self->connection;

    $connection->_watch_can_write unless $connection->_watching_can_write;

    return;

}

sub disconnect {

    my ($self, @args) = @_;

    return $self->connection->_disconnected( @args );

}

sub can_write {

    my ($self) = @_;

    my $connection = $self->connection;

    my $queue = $connection->_write_queue;

    my $write = $queue->[0];

    my ($buffer_ref, $callback) = @$write{ qw ( buffer_ref callback ) };

    my $write_progress = $self->{write_progress};

    my $to_write = (length $$buffer_ref) - $write_progress;

    my ($error, $written) = $connection->_socket_write( $buffer_ref, $to_write, $write_progress );

    if ($error) {

        $connection->_disconnected( "Write error: $error" );

        return;

    } else {

        if ($written == $to_write){

            $self->{write_progress} = 0;

            shift @$queue;

            call( $callback );

        } else {

            $self->{write_progress} = $write_progress + $written;

        }

    }

    $connection->_unwatch_can_write unless @$queue;

    return;

}

sub can_write_timeout {

    my ($self) = @_;

    $self->connection->_disconnected( 'Write timeout' );

    return;
}

sub can_read {

    my ($self)= @_;

    my $connection = $self->connection;

    my ($error, $data_ref) = $connection->_socket_read;

    if ($error) {

        $connection->_disconnected( "Read error: $error" );

    } else {

        $connection->_on_read( $data_ref ) if $data_ref;

    }

    return;

}

1;