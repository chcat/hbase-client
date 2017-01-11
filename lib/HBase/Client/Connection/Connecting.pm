package HBase::Client::Connection::Connecting;

use v5.14;
use warnings;

use parent HBase::Client::Connection::State;

sub _enter {

    my ($self, %args) = @_;

    my $callback = $args{callback};

    $self->_watch_can_write_once( sub {

            $self->_state( 'HBase::Client::Connection::Connected' );

            $callback->();

        }, $args{timeout}, sub {

            $self->_state( 'HBase::Client::Connection::Disconnected' );

            $callback->( "Could not connect: timeout" );

        } );

    return;

}

1;