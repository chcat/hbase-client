package HBase::Client::Connection::State;

use v5.14;
use warnings;

use AnyEvent;
use Errno;
use IO::Socket::INET;
use Scalar::Util qw( blessed );
use Socket qw( TCP_NODELAY IPPROTO_TCP SOL_SOCKET SO_KEEPALIVE );

require HBase::Client::Connection::Opened;
require HBase::Client::Connection::Connecting;
require HBase::Client::Connection::Connected;
require HBase::Client::Connection::Disconnected;
require HBase::Client::Connection::Closed;

# PUBLIC INSTANCE METHODS

sub write {

    my ($self, $callback, $data_ref) = @_;

    my $queue = $self->{write_queue};

    my $write = {
            buffer_ref => $data_ref,
            callback   => $callback
        };

    push @$queue, $write;

    return;

}

sub close {

    my ($self) = @_;

    $self->_state( 'HBase::Client::Connection::Closed' );

    my $queue = $self->{write_queue};

    for my $write ( @$queue ){

        $write->{callback}->( 'Connection closed' );

    }

    return;

}

sub set_on_read { $_[0]->{on_read} = $_[1]; }

sub set_on_disconnect { $_[0]->{on_disconnect} = $_[1]; }

# PROTECTED CLASS METHODS

sub _new {

    my ($class, %args) = @_;

    return bless {

            host                => $args{host},
            port                => $args{port},

            read_buffer_size    => $args{read_buffer_size} // 1024*64,

            on_read             => $args{on_read},
            on_disconnect       => $args{on_disconnect},

            connect_timeout     => $args{connect_timeout} // 3,
            write_timeout       => $args{write_timeout} // 3,

            write_queue         => [],

        }, $class;

}

# PROTECTED INSTANCE METHODS

sub _enter {}

sub _leave {}

sub _state {

    my ($self, $new_state, $enter_args, $leave_args) = @_;

    my $old_state = blessed $self;

    $self->_leave( $new_state, $leave_args ? @$leave_args : () );

    bless $self, $new_state;

    $self->_enter( $old_state, $enter_args ? @$enter_args : () );

    return;
}

sub _watching_can_write { defined shift->{write_watcher}; }

sub _unwatch_can_write{ @{+shift}{qw (write_watcher write_timeout_watcher)} = (); }

sub _unwatch_can_read { undef shift->{read_watcher}; }

sub _on_read { shift->_on_event( 'on_read', @_ ); }

sub _on_disconnect { shift->_on_event( 'on_disconnect', @_  ); }

sub _on_event {

    my $callback = shift->{+shift};

    $callback->( @_ ) if $callback;

    return;

}

sub _watch_can_write { shift->_watch_io( 'w', 0, @_ ); }

sub _watch_can_write_once { shift->_watch_io( 'w', 1, @_ ); }

sub _watch_can_read { shift->_watch_io( 'r', 0, @_ ); }

sub _watch_io {

    my ($self, $type, $once, $io_callback, $timeout, $timeout_callback) = @_;

    state $types = { w => 'write', r => 'read' };

    my ($watcher_name, $timeout_watcher_name) = map { $types->{ $type } . $_ } qw( _watcher _timeout_watcher );

    $self->_setup_io_timeout_watcher( $timeout, $timeout_callback, $timeout_watcher_name, $watcher_name );

    $self->{ $watcher_name } = AnyEvent->io( poll => $type, fh => $self->{socket}->fileno, cb => sub {

            undef $self->{ $watcher_name } if $once;

            # cancel the corresponding timeout watcher
            undef $self->{ $timeout_watcher_name };

            # refresh the corresponding io timeout timer on each step of continious io operation
            $self->_setup_io_timeout_watcher( $timeout, $timeout_callback, $timeout_watcher_name, $watcher_name ) unless $once;

            $io_callback->();

        } );

    return;

}

sub _setup_io_timeout_watcher {

    my ($self, $timeout, $callback, $timeout_watcher_name, $io_watcher_name ) = @_;

    AnyEvent->now_update; # updates AnyEvent's "current time" - otherwise the timer we gonna set up may fire too early

    $self->{ $timeout_watcher_name } = AnyEvent->timer( after => $timeout, cb => sub {

            # cancel the corresponding io watcher as timeout fired
            undef $self->{ $io_watcher_name };

            undef $self->{ $timeout_watcher_name };

            $callback->();

        } ) if $timeout;

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
            ) or return "$@";

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


