package HBase::Client::Connection;

use v5.14;
use warnings;

use HBase::Client::Connection;
use HBase::Client::Proto::Loader;
use HBase::Client::Proto::Utils;
use JSON::XS;
use Promises qw( deferred );

my $JSON = JSON->new->utf8(1)->canonical(1)->convert_blessed(1);

sub new {

    my ($class, %args)= @_;

    my $self = bless {
            call_count  => 0,
            connection  => $args{connection},
            read_buffer => '',
            calls       => {},
        }, $class;

    return $self;

}

sub call {

    my ( $self, $call ) = @_;

    $self->{calls}->{$self->{call_count}++} = {

            deferred => deferred,

        };



}

sub _handshake {

    my ( $self )= @_;

    my $greeting = pack ('a*CC', 'HBas', 0, 80); # preamble

    my $connection_header = HBase::Client::Proto::ConnectionHeader->new;

    $connection_header->set_service_name("ClientService");

    # $connection_header->set_user_info();TODO

    $greeting .= $self->_frame_data( $connection_header->encode );

    $self->{connection}->write( $greeting, sub { $self->_connected() } );

}

sub _connected {

    $self->{connected} = 1;

}

sub _on_read {

    my ( $self, $data ) = @_;

    my $self->{read_buffer} .= $data;

    while (defined (my $frame = $self->_read_frame()){

        my $header = HBase::Client::Proto::ResponseHeader->decode( substr( $self->{read_buffer}, 0, HBase::Client::Proto::Utils::read_varint( $self->{read_buffer} ), '' ) );

        if ( $header->has_call_id() && my $call = delete $self->{calls}->{ $header->get_call_id() } ){

            if ( $header->has_exception() ){

                $call->{deferred}->reject( $header->get_exception() );

            } else {

                $call->{deferred}->resolve( $frame );

            }

        } else {

            # Got a response to a call that we forgot or never did. TODO

            warn $header->encode_json(); #TODO

        }

    }

}

sub _read_frame {

    $_[0]->{read_await} = unpack( 'N', substr( $_[0]->{read_buffer}, 0, 4, '' ) )
        if !defined $_[0]->{read_await} && length $_[0]->{read_buffer} >= 4;

    return defined $_[0]->{read_await} && length $_[0]->{read_buffer} >= $_[0]->{read_await}
        ? substr( $_[0]->{read_buffer}, 0, delete $_[0]->{read_await}, '' )
        : undef;

}

sub _frame_data {

    return pack ('Na*', length $_[1], $_[1]);

}

1;