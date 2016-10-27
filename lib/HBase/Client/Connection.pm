package HBase::Client::Connection;

use v5.14;
use warnings;

use IO::Socket::INET;
use IO::Socket::Timeout;
use Socket qw( TCP_NODELAY IPPROTO_TCP SOL_SOCKET SO_KEEPALIVE );

use HBase::Client::Proto::Loader;

sub connect {

    my ( $class, $host, $port, $args )= @_;

    my $socket= IO::Socket::INET->new(
            PeerAddr    =>  $host,
            PeerPort    =>  $port,
            Proto       => 'tcp',
            Timeout     => 10,          # TODO
        ) or die "Can't connect: $@";

    $socket->setsockopt( SOL_SOCKET, SO_KEEPALIVE, 1 );

    IO::Socket::Timeout->enable_timeouts_on( $socket );
    $socket->read_timeout( 5 );
    $socket->write_timeout( 5 );

    my $self = bless {
            socket      => $socket,
        }, $class;

    $self->_say_hello;

    return $self;

}

sub _say_hello {

    my ( $self )= @_;

    {
        local $\ if defined $\;

        print $self->{socket}, pack ('a*CC', 'HBas', 0, 80);
    }

    my $ch = HBase::Client::Proto::ConnectionHeader->new;

    $ch->set_service_name("ClientService");

    $self->send_message( $ch );

}

sub read {

    my ( $self )= @_;

    my $buf;
    my $read_cnt;
    my $read_total_cnt;

    do {

        $read_cnt = sysread($self->{socket}, $buf, 1024, $read_total_cnt);
        $read_total_cnt += $read_cnt;

    } while ($read_cnt > 0);

    return $buf;

}

sub send_message {

    my ( $self, $message )= @_;

    local $\ if defined $\;

    my $buf = $message->encode;

    print $self->{socket}, pack ('Na*', bytes::length $buf, $buf );

}


1;