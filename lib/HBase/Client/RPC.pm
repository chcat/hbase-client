package HBase::Client::RPC;

use v5.14;
use warnings;

use AnyEvent;

use HBase::Client::Proto::Loader;
use HBase::Client::Proto::Utils qw( read_varint encode_varint );

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

    my $timeout = exists $options->{timeout} ? $options->{timeout} : $self->{timeout};

    $self->{calls}->{$call_id} = {

            deferred => $deferred,

            method   => $method,

            timeout_watcher  => $timeout ? AnyEvent->timer( after => $timeout, cb => sub { $self->_timeout_call( $call_id ) } ) : undef,

        };

    my @messages = ( HBase::Client::Proto::RequestHeader->new( {

            call_id         => $call_id,

            method_name     => $method->{name},

            request_param   => $param ? 1 : 0,

        } ), $param // () );

    $self->_write_as_frame( $self->_pack_delimited( @messages ) );

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

sub _connected {

    $_[0]->{connected} = 1;

}

sub _pack_delimited {

    my ( $self, @messages ) = @_;

    return $self->_join_delimited( [ map { defined $_ ? $_->encode : () } @messages ] );

}

sub _join_delimited {

    my ( $self, $pieces ) = @_;

    return join '', map { ( encode_varint( length $_ ), $_ ) } @$pieces;

}

sub _write_as_frame {

    $_[0]->{connection}->write( undef, \$_[0]->_make_frame( $_[1] ) );

}

sub _make_frame {

    return pack ('Na*', length $_[1], $_[1]);

}

sub _on_read {

    my ( $self, $data ) = @_;

    $self->{read_buffer} .= $data;

    while (defined (my $data = $self->_try_read_framed())){

        my ($header_enc, $response_enc, $rest_enc) = $self->_split_delimited( $data );

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

sub _split_delimited {

    my ( $self ) = @_;

    my @pieces;

    push @pieces, substr( $_[1], 0, read_varint( $_[1] ), '' ) while length $_[1];

    return @pieces;
}

sub _try_read_framed {

    $_[0]->{frame_length} = $_[0]->_try_read_int() if !defined $_[0]->{frame_length};

    return defined $_[0]->{frame_length} && $_[0]->_can_read_bytes( $_[0]->{frame_length} )
        ? $_[0]->_read_bytes( delete $_[0]->{frame_length} )
        : undef;

}

sub _try_read_int {

    return $_[0]->_can_read_bytes(4) ? unpack( 'N', $_[0]->_read_bytes(4)) : undef;

}

sub _read_bytes {

    return substr( $_[0]->{read_buffer}, 0, $_[1], '' );

}

sub _can_read_bytes {

     return length $_[0]->{read_buffer} >= $_[1];

}

1;