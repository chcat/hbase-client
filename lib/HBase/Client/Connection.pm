package HBase::Client::Connection;

use v5.14;
use warnings;

use AnyEvent;
use Errno qw( EINTR EPIPE );
use IO::Socket::INET;
use Socket qw( TCP_NODELAY IPPROTO_TCP SOL_SOCKET SO_KEEPALIVE );

use HBase::Client::Proto::Loader;

sub new {

    my ($class, %args)= @_;

    my $socket;

    {
        local $@;

        $socket = IO::Socket::INET->new(
                PeerAddr    =>  $args{host},
                PeerPort    =>  $args{port},
                Proto       => 'tcp',
                Blocking    => 0,
            );

        $socket->setsockopt( SOL_SOCKET, SO_KEEPALIVE, 1 );
        $socket->setsockopt( IPPROTO_TCP, TCP_NODELAY, 1 );
    }

    my $self = bless {
            socket         => $socket,
            on_read        => $args{on_read},
            write_queue    => [],
            write_progress => 0,
        }, $class;

    $self->_watch_can_read();

    return $self;
}

sub _watch_can_read {

    my ($self) = @_;

    $self->{read_watcher} = $self->_watch( 'r', sub { $self->_can_read } );

}

sub _unwatch_can_read {

    undef $_[0]->{read_watcher};

}

sub _watch_can_write {

    my ($self) = @_;

    $self->{write_watcher} = $self->_watch( 'w', sub { $self->_can_write } );

}

sub _unwatch_can_write {

    undef $_[0]->{write_watcher};

}

sub _watch {

    return AnyEvent->io( poll => $_[1], fh => $_[0]->{socket}->fileno, cb => $_[2] );

}

sub _can_read {

    my ( $self )= @_;

    my $buf;

    my $read = sysread( $self->{socket}, $buf, 1024*64);

    if (defined $read){

        $self->{on_read}->($buf) if $self->{on_read};

    } else {

        my $error= $!;

        die $error; # TODO

    }

}

sub on_read {

    $_[0]->{on_read} = $_[1];

}

sub _can_write {

    my ($self) = @_;

    my $queue = $self->{write_queue};

    if (@$queue){

        my ($buffer_ref, $cb) = @{$queue->[0]}{ qw ( buffer_ref cb ) };

        my $to_write = length $$buffer_ref - $self->{write_progress};

        my $written = syswrite( $self->{socket}, $$buffer_ref, $to_write );

        if (defined $written){

            if ($written == $to_write){

                $self->{write_progress} = 0;

                shift $queue;

                $cb->();

            } else {

                $self->{write_progress} += $written;

            }

        } else {

            my $error= $!;

            # TODO report using the $cb?

            die $error;

        }

    }

    _unwatch_can_write() unless @$queue;

}

sub write {

    my ($self, $buffer_ref, $cb) = @_;

    $self->_watch_can_write() if push( $self->{write_queue}, { buffer_ref => $buffer_ref, cb => $cb } ) == 1;

}

1;