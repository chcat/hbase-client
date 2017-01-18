package HBase::Client::Connection::Connected;

use v5.14;
use warnings;

use parent 'HBase::Client::Connection::State';

sub _enter {

    my ($self) = @_;

    $self->{write_progress} = 0;

    $self->_watch_can_read( sub {

            $self->_can_read();

        } );

    $self->_setup_write_watcher if @{$self->{write_queue}};

    return;

}

sub write {

    my $self = shift;

    $self->SUPER::write( @_ );

    $self->_setup_write_watcher unless $self->_watching_can_write;

    return;

}

sub _setup_write_watcher {

     my ($self) = @_;

     $self->_watch_can_write( sub {

             $self->_can_write;

         }, $self->{write_timeout}, sub {

             $self->_disconnect("Write timeout");

         } );

     return;
}

sub _can_write {

    my ($self) = @_;

    my $queue = $self->{write_queue};

    my $write = $queue->[0];

    my ($buffer_ref, $callback) = @$write{ qw ( buffer_ref callback ) };

    my $write_progress = $self->{write_progress};

    my $to_write = (length $$buffer_ref) - $write_progress;

    my ($error, $written) = $self->_socket_write( $buffer_ref, $to_write, $write_progress );

    if ($error) {

        return $self->_disconnect( $error );

    } else {

        if ($written == $to_write){

            $self->{write_progress} = 0;

            shift @$queue;

            $callback->() if $callback;

        } else {

            $self->{write_progress} = $write_progress + $written;

        }

    }

    $self->_unwatch_can_write unless @$queue;

}

sub _can_read {

    my ( $self )= @_;

    my ($error, $data_ref) = $self->_socket_read;

    if ($error) {

        $self->_disconnect( $error );

    } else {

        $self->_on_read( $data_ref ) if $data_ref;

    }

    return;

}

sub _disconnect {

    my ( $self, $reason )= @_;

    $self->_state( 'HBase::Client::Connection::Disconnected' );

    $self->_on_disconnect( $reason );

    return;

}

1;