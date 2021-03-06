package HBase::Client::RPCChannel;

use v5.14;
use warnings;

use AnyEvent;
use HBase::Client::Proto::Loader;
use HBase::Client::Proto::Utils qw( split_delimited join_delimited );
use HBase::Client::Error qw(
        connection_error
        timeout_error
        exception_error
    );
use Promises qw( deferred );


sub new {

    my ($class, %args)= @_;

    my $connection = $args{connection};

    my $self = bless {
            call_count      => 0,
            calls           => {},
            connection      => $connection,
            read_buffer     => '',
            timeout         => $args{timeout} // 3,
            connected       => undef,
            disconnected    => undef,

        }, $class;

    $connection->set_callbacks(
            on_read         => sub { $self->_on_read( @_ ) },
            on_disconnect   => sub { $self->_on_disconnect( @_ ) },
        );

    return $self;

}

sub disconnect {

    my ($self, $reason) = @_;

    $self->{connection}->disconnect( $reason ) if $self->{connected};

    return;

}

sub disconnected {

    my ($self) = @_;

    return $self->{disconnected}->promise;

}

sub _on_disconnect {

    my ($self, $reason) = @_;

    undef $self->{connected};

    my $disconnected = delete $self->{disconnected};

    $disconnected->resolve( $reason ) if $disconnected;

    while (my ($call_id, $call) = each %{$self->{calls}}) {
        delete $self->{calls}->{ $call_id };
        undef $call->{timeout_watcher};
        $call->{deferred}->reject( connection_error( $reason ) );
    }

    return;
}

sub connect {

    my ($self) = @_;

    return $self->{connected} //= $self->_connect;

}

sub _connect {

    my ($self) = @_;

    my $deferred = deferred;

    $self->{connection}->connect( sub {

            my ($error) = @_;

            if ($error){

                $deferred->reject( connection_error( $error ) );

            } else{

                $self->_write_connection_header( $deferred );

            }

        } );

    return $deferred->promise
        ->then( sub {

                $self->{disconnected} = deferred;

                return $self;

            }, sub {

                my ($error) = @_;

                undef $self->{connected};

                die connection_error( $error );

            } );

}

sub _write_connection_header {

    my ( $self, $deferred ) = @_;

    my $header = HBase::Client::Proto::ConnectionHeader->new( {
            service_name => 'ClientService',
            user_info    => {

                    effective_user  => 'Gandalf', #TODO

                },

        } )->encode;

    my $greeting = pack ('a*CCNa*', 'HBas', 0, 80, length $header, $header);

    $self->{connection}->write( sub {

            my ($error) = @_;

            if ($error){

                $deferred->reject( connection_error( $error ) );

            } else {

                $deferred->resolve;

            }

        }, \$greeting );

    return;

}

sub make_call {

    my ( $self, $call, $options ) = @_;

    my $deferred = deferred;

    my $call_id = $self->{call_count}++;

    my $call_entry = $self->{calls}->{$call_id} = {
            deferred         => $deferred,
            response_type    => $call->{response_type},
            stats            => $options->{stats},
        };

    my $param = $call->{param};

    my @messages = ( HBase::Client::Proto::RequestHeader->new( {
            call_id         => $call_id,
            method_name     => $call->{method},
            request_param   => $param ? 1 : 0,
        } ) );

    push @messages, $param if $param;

    $self->_write_as_frame( sub {

            if (my $timeout = $options->{timeout} // $self->{timeout}){

                AnyEvent->now_update; # updates AnyEvent's "current time" - otherwise the timer we gonna set up may fire too early

                $call_entry->{timeout_watcher} = AnyEvent->timer( after => $timeout, cb => sub { $self->_timeout_call( $call_id ) } );

            }

        }, $options->{stats}, @messages );

    return $deferred->promise;

}

sub _timeout_call {

    my ($self, $call_id) = @_;

    if ( my $call = delete $self->{calls}->{ $call_id } ){

        $call->{deferred}->reject( timeout_error( 'RPC call timeout' ) );

    }

    return;

}

sub _write_as_frame {

    my ($self, $cb, $stats, @messages) = @_;

    my $frame_ref = join_delimited( [ map { defined $_ ? $_->encode : () } @messages ] );

    my $length = length $$frame_ref;

    substr($$frame_ref, 0, 0) = pack('N', $length);

    $self->{connection}->write( sub { $stats->{written} = $length + 4; $cb->() if $cb; }, $frame_ref );

}

sub _on_read {

    my ( $self, $data_ref ) = @_;

    $self->{read_buffer} .= $$data_ref;

    while (my $frame_ref = $self->_try_read_frame){

        my $frame_length = length $$frame_ref;

        my ($header_enc, $response_enc, $rest_enc) = @{split_delimited( $frame_ref )};

        my $header = HBase::Client::Proto::ResponseHeader->decode( $header_enc );

        if ( $header->has_call_id and my $call = delete $self->{calls}->{ $header->get_call_id } ){

            undef $call->{timeout_watcher};

            $call->{stats}->{read} = $frame_length;

            my $deferred = $call->{deferred};

            if ( $header->has_exception ){

                $deferred->reject( exception_error( $header->get_exception ) );

            } else {

                $deferred->resolve( $call->{response_type}->decode( $response_enc ) );

            }

        } else {

            # Got a response to a call that we either considered failed already or never did. TODO

            warn $header->encode_json; #TODO

        }

    }

}

sub _try_read_frame {

    my ($self) = @_;

    my $frame_length = $self->{frame_length} //= $self->_try_read_int;

    my $frame_ref = $self->_try_read_bytes( $frame_length );

    undef $self->{frame_length} if $frame_ref;

    return $frame_ref;

}

sub _try_read_int {

    my $bytes_ref = $_[0]->_try_read_bytes(4);

    return $bytes_ref ? unpack( 'N', $$bytes_ref ) : undef;

}

sub _try_read_bytes {  return $_[0]->_can_read_bytes( $_[1] ) ? $_[0]->_read_bytes( $_[1] ) : undef; }

sub _read_bytes { return \substr( $_[0]->{read_buffer}, 0, $_[1], '' ); }

sub _can_read_bytes { return defined $_[1] && length $_[0]->{read_buffer} >= $_[1]; }

1;