package HBase::Client::Connection;

use v5.14;
use warnings;

use AnyEvent;
use Errno;
use IO::Socket::INET;
use Socket qw( TCP_NODELAY IPPROTO_TCP SOL_SOCKET SO_KEEPALIVE );

use HBase::Client::Connection::Connecting;
use HBase::Client::Connection::Connected;
use HBase::Client::Connection::Disconnected;

sub new {

    my ($class, %args) = @_;

    my $self = bless {

            host                 => $args{host},
            port                 => $args{port},

            read_buffer_size     => $args{read_buffer_size} // 1024*64,

            on_read              => $args{on_read},
            on_connect           => $args{on_connect},
            on_disconnect        => $args{on_disconnect},

            connect_timeout      => $args{connect_timeout} // 3,
            write_timeout        => $args{write_timeout} // 3,

        }, $class;

    $self->{state} = HBase::Client::Connection::Disconnected->new( connection => $self );

    return $self;

}

sub set_callbacks {

    my ($self, %args) = @_;

    state $callbacks = [qw( on_read on_connect on_disconnect )];

    @$self{ @$callbacks } = @args{ @$callbacks };

    return;

}

sub connect {

    my ($self, $callback, %args) = @_;

    return $self->_state->connect( $callback, timeout => $args{timeout} // $self->{connect_timeout} );

}

sub write {

    my ($self, $callback, $data_ref) = @_;

    return $self->_state->write( $callback, $data_ref );

}

sub disconnect {

    my ($self, $reason) = @_;

    return $self->_state->disconnect( $reason );

}

sub _connecting {

    my ($self, $callback, %args) = @_;

    $self->_state( 'HBase::Client::Connection::Connecting', $callback, %args );

    return;

}

sub _connected {

    my ($self, $callback) = @_;

    $self->_state( 'HBase::Client::Connection::Connected' );

    $callback->() if $callback;

    $self->{on_connect}->() if $self->{on_connect};

    return;

}

sub _disconnected {

    my ($self, $reason, $callback) = @_;

    $self->_state( 'HBase::Client::Connection::Disconnected' );

    $callback->( $reason ) if $callback;

    $self->{on_disconnect}->( $reason ) if $self->{on_disconnect};

    return;

}

sub _state {

    my ($self, $state_class, @enter_args) = @_;

    return $self->{state} unless $state_class;

    my $state = $state_class->new( connection => $self );

    $self->{state} = $state;

    return $state->enter(@enter_args);

}

sub _on_read {

    my ($self, $data_ref) = @_;

    $self->{on_read}->( $data_ref ) if $self->{on_read};

    return;

}

sub _watching_can_write { defined $_[0]->{write_watcher} }

sub _unwatch_can_write{ @{$_[0]}{qw (write_watcher write_timeout_watcher)} = () }

sub _watch_can_write {

    my ($self, $timeout) = @_;

    $timeout //= $self->{write_timeout};

    $self->_renew_write_timeout( $timeout );

    $self->{ write_watcher } = AnyEvent->io( poll => 'w', fh => $self->{socket}->fileno, cb => sub {

            $self->_renew_write_timeout( $timeout ); # TODO renew the timer AFTER can_write completes, if the watcher is still the same

            $self->_state->can_write;

            return;

        } );

    return;

}

sub _renew_write_timeout {

    my ($self, $timeout) = @_;

    undef $self->{ write_timeout_watcher }; # clear existing watcher

    return unless $timeout;

    AnyEvent->now_update; # updates AnyEvent's "current time" - otherwise the timer we gonna set up may fire too early

    $self->{ write_timeout_watcher } = AnyEvent->timer( after => $timeout, cb => sub {

            # It could have happen that the timeout is caused by the consumer code blocking the event loop that prevented
            # AnyEvent's ready-to-write check. So we check if the socket is really broken.

            my ($error) = $self->_socket_write(\'',0,0);

            if ($error){

                $self->_unwatch_can_write;

                $self->_state->can_write_timeout;

            } else {

                $self->_renew_write_timeout(1); # give it another chance

            }

            return;

        } );

    return;

}

sub _unwatch_can_read { undef $_[0]->{read_watcher}; }

sub _watch_can_read {

    my ($self) = @_;

    $self->{ read_watcher } = AnyEvent->io( poll => 'r', fh => $self->{socket}->fileno, cb => sub {

            $self->_state->can_read;

            return;

        } );

    return;
}

sub _open_socket {

    my ($self) = @_;

    my $socket;

    {
        local $@;

        $socket = IO::Socket::INET->new(
                PeerAddr    =>  $self->{host},
                PeerPort    =>  $self->{port},
                Proto       => 'tcp',
                Blocking    => 0,
            ) or
            return "Error opening a socket: $@";

        $socket->setsockopt( SOL_SOCKET, SO_KEEPALIVE, 1 );
        $socket->setsockopt( IPPROTO_TCP, TCP_NODELAY, 1 );
    }

    $self->{socket} = $socket;

    return;

}

sub _socket_write {

    my ($self, $buffer_ref, $length, $offset) = @_;

    my $written = syswrite( $self->{socket}, $$buffer_ref, $length, $offset );

    my $error = ( defined $written or $!{EAGAIN} ) ? undef : "$!";

    return ($error, $written // 0);
}

sub _socket_read {

    my ($self) = @_;

    my $buffer;

    my $read = sysread( $self->{socket}, $buffer, $self->{read_buffer_size} );

    if ($read){

        return (undef, \$buffer);

    } elsif (!defined $read) {

        return $!{EAGAIN} ? undef : "$!";

    } else { # $read == 0

        return "Disconnected from the server";

    }

}

sub _close_socket {

    my ($self) = @_;

    if (my $socket = delete $self->{socket}){

        $self->_unwatch_can_write;

        $self->_unwatch_can_read;

        $socket->close;

    }

}

1;