package HBase::Client::Connection::State;

use v5.14;
use warnings;

use AnyEvent;

require HBase::Client::Connection::Opened;
require HBase::Client::Connection::Connecting;
require HBase::Client::Connection::Connected;
require HBase::Client::Connection::Disconnected;
require HBase::Client::Connection::Closed;

sub connect {

    my $state = blessed shift;

    die "Invalid connection state $state";

}

sub close {

    my ($self) = @_;

    $self->_state( 'HBase::Client::Connection::Closed' );

    return;

}

sub write {

    my ($self, $callback, $data_ref) = @_;

    my $queue = $self->{write_queue};

    my $write = {
            buffer_ref => $data_ref,
            callback         => $callback
        };

    push @$queue, $write;

    return;

}

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

sub _state {

    my ($self, $new_state, @args) = @_;

    $self->_leave();

    bless $self, $new_state;

    $self->_enter( @args );

    return;
}

sub _enter {}

sub _leave {}

sub _watching_can_write { defined shift->{write_watcher}; }

sub _unwatch_can_write { undef shift->{write_watcher}; }

sub _unwatch_can_write_timeout { undef shift->{write_timeout_watcher}; }

sub _unwatch_can_read { undef shift->{read_watcher}; }

sub _on_read { shift->_on_event( 'on_read' ); }

sub _on_disconnect { shift->_on_event( 'on_disconnect' ); }

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

            undef $self->{ $timeout_watcher_name };

            $self->_setup_io_timeout_watcher( $timeout, $timeout_callback, $timeout_watcher_name, $watcher_name ) unless $once; # refresh io timeout timer

            $io_callback->();

        } );

    return;

}

sub _setup_io_timeout_watcher {

    my ($self, $timeout, $callback, $timeout_watcher_name, $io_watcher_name ) = @_;

    $self->{ $timeout_watcher_name } = AnyEvent->timer( after => $timeout, cb => sub {

            undef $self->{ $io_watcher_name };

            undef $self->{ $timeout_watcher_name };

            $callback->();

        } ) if $timeout;

}

1;


