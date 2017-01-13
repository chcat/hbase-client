package HBase::Client::Connection::Connecting;

use v5.14;
use warnings;

use parent 'HBase::Client::Connection::State';

sub _enter {

    my ($self, $previous_state, $callback, %args) = @_;

    if (my $error = $self->_open_socket) {

        $self->_state( 'HBase::Client::Connection::Disconnected' );

        $callback->( "Could not connect: $error" ) if $callback;

        return;

    }

    my $timeout = $args{timeout} // $self->{connect_timeout};

    $self->_watch_can_write_once( sub {

            $self->_state( 'HBase::Client::Connection::Connected' );

            $callback->() if $callback;

        }, $timeout, sub {

            $self->_state( 'HBase::Client::Connection::Disconnected' );

            $callback->( "Could not connect: timeout" ) if $callback;

        } );

    return;

}

1;