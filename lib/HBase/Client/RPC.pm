package HBase::Client::RPC;

use v5.14;
use warnings;

use AnyEvent;
use HBase::Client::Proto::Loader;
use HBase::Client::Proto::Utils qw( split_delimited join_delimited );
use Promises qw( deferred );

sub new {

    my ($class, %args)= @_;

    my $connection = $args{connection};

    my $self = bless {
            call_count  => 0,
            calls       => {},
            connection  => $connection,
            read_buffer => '',
            timeout     => $args{timeout} // 3,
        }, $class;

    $connection->set_on_read( sub { $self->_on_read( @_ ) } );

    $connection->connect();

    $self->_handshake();

    return $self;

}

sub call_async {

    my ( $self, $method, $param, $options ) = @_;

    my $deferred = deferred;

    my $call_id = $self->{call_count}++;

    my $timeout = $options->{timeout} // $self->{timeout};

    $self->{calls}->{$call_id} = {
            deferred => $deferred,
            method   => $method,
            timeout_watcher  => $timeout ? AnyEvent->timer( after => $timeout, cb => sub { $self->_timeout_call( $call_id ) } ) : undef,
        };

    my @messages = ( HBase::Client::Proto::RequestHeader->new( {
            call_id         => $call_id,
            method_name     => $method->{name},
            request_param   => $param ? 1 : 0,

        } ) );

    push @messages, $param if $param;

    $self->_write_frame( $self->_pack_delimited( @messages ) );

    return $deferred->promise();

}

sub _timeout_call {

    my ($self, $call_id) = @_;

    if ( my $call = delete $self->{calls}->{ $call_id } ){

            $call->{deferred}->reject('TIMEOUT');

    }

}

sub _handshake {

    my ( $self )= @_;

    my $greeting = pack ('a*CC', 'HBas', 0, 80); # preamble

    my $connection_header = HBase::Client::Proto::ConnectionHeader->new( {
            service_name => 'ClientService',
            user_info    => {

                    effective_user  => 'Gandalf', #TODO

                },

        } );

    $greeting .= $self->_make_frame( $connection_header->encode );

    $self->{connection}->write( sub { $self->_connected() }, \$greeting );

}

sub _connected { $_[0]->{connected} = 1; }

sub _pack_delimited {

    my ( $self, @messages ) = @_;

    return join_delimited( [ map { defined $_ ? $_->encode : () } @messages ] );

}


sub _write_frame { $_[0]->{connection}->write( undef, \$_[0]->_make_frame( $_[1] ) ); }

sub _make_frame { return pack ('Na*', length $_[1], $_[1]); }

sub _on_read {

    my ( $self, $data_ref ) = @_;

    $self->{read_buffer} .= $$data_ref;

    while (defined (my $frame = $self->_try_read_frame)){

        my ($header_enc, $response_enc, $rest_enc) = @{_split_delimited( $frame )};

        my $header = HBase::Client::Proto::ResponseHeader->decode( $header_enc );

        if ( $header->has_call_id() and my $call = delete $self->{calls}->{ $header->get_call_id() } ){

            undef $call->{timeout_watcher};

            my $deferred = $call->{deferred};

            if ( $header->has_exception() ){

                $deferred->reject( $header->get_exception() );

            } else {

                $deferred->resolve( $call->{method}->{response_type}->decode( $response_enc ) );

            }

        } else {

            # Got a response to a call that we forgot or never did. TODO

            warn $header->encode_json(); #TODO

        }

    }

}

sub _try_read_frame {

    my ($self) = @_;

    my $frame_length = $self->{frame_length} //= $self->_try_read_int();

    if (defined (my $bytes = $self->_try_read_bytes( $frame_length ))){

        undef $self->{frame_length};

        return $bytes;

    }

    return undef;

}

sub _try_read_int {

    my $bytes = $_[0]->_try_read_bytes(4);

    return defined $bytes ? unpack( 'N', $bytes ) : undef;

}

sub _try_read_bytes {  return $_[0]->_can_read_bytes( $_[1] ) ? $_[0]->_read_bytes( $_[1] ) : undef; }

sub _read_bytes { return substr( $_[0]->{read_buffer}, 0, $_[1], '' ); }

sub _can_read_bytes { return length $_[0]->{read_buffer} >= $_[1]; }

1;