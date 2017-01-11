package HBase::Client::Connection::Disconnected;

use v5.14;
use warnings;

use parent HBase::Client::Connection::State;

use IO::Socket::INET;
use Socket qw( TCP_NODELAY IPPROTO_TCP SOL_SOCKET SO_KEEPALIVE );

sub connect {

    my ($self, $callback, %args) = @_;

    my $socket;

    {
        local $@;

        $socket = IO::Socket::INET->new(
                PeerAddr    =>  $self->{host},
                PeerPort    =>  $self->{port},
                Proto       => 'tcp',
                Blocking    => 0,
            );

        unless ($socket) {

            $callback->( "Could not connect: $@" );

            return;

        }

        $socket->setsockopt( SOL_SOCKET, SO_KEEPALIVE, 1 );
        $socket->setsockopt( IPPROTO_TCP, TCP_NODELAY, 1 );
    }

    $self->{socket} = $socket;

    $self->_state( 'HBase::Client::Connection::Connecting',
            callback => $callback,
            timeout  => $args{connect_timeout} // $self->{connect_timeout},
        );

    return;
}

1;