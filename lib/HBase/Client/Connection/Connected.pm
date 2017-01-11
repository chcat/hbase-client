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

    $self->_setup_write_watcher if @{$self->{write_queue}}; # start writing after reconnect

    return;

}

sub _leave {

    my ($self) = @_;

    $self->_unwatch_can_write;
    $self->_unwatch_can_write_timeout;
    $self->_unwatch_can_read;

    $self->{socket}->close;

}

sub write {

    my $self = shift;

    $self->SUPER::write( @_ );

    $self->_setup_write_watcher unless $self->_watching_can_write;

}

sub _setup_write_watcher {

     my ($self) = @_;

     $self->_watch_can_write( $self->{write_timeout}, sub {

             $self->_can_write;

         }, sub {

             $self->_disconnect("Write timeout");

         } )

}

sub _can_write {

    my ($self) = @_;

    my $queue = $self->{write_queue};

    my $write = $queue->[0];

    my ($buffer_ref, $callback) = @$write{ qw ( buffer_ref callback ) };

    my $write_progress = $self->{write_progress};

    my $to_write = (length $$buffer_ref) - $write_progress;

    my $written = syswrite( $self->{socket}, $$buffer_ref, $to_write, $write_progress );

    if (defined $written){

        if ($written == $to_write){

            $self->{write_progress} = 0;

            shift @$queue;

            $callback->() if $callback;

        } else {

            $self->{write_progress} = $write_progress + $written;

        }

    } else {

        $self->_disconnect( $! ) unless $!{EAGAIN};

    }

    $self->_unwatch_can_write unless @$queue;

}

sub _can_read {

    my ( $self )= @_;

    my $buf;

    my $read = sysread( $self->{socket}, $buf, $self->{read_buffer_size} );

    if ($read){

        $self->_on_read( $buf );

    } elsif (!defined $read) {

        $self->_disconnect( $! ) unless $!{EAGAIN};

    } else { # $read == 0

        $self->_disconnect("Disconnected from the server");

    }

}

sub _disconnect {

    my ( $self, $reason )= @_;

    $self->_state( 'HBase::Client::Connection::Disconnected' );

    $self->_on_disconnect( $reason );

}

1;