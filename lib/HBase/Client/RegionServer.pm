package HBase::Client::RegionServer;

use v5.14;
use warnings;

use Digest::MD5 qw( md5_hex );

use List::Util qw(first);

use HBase::Client::Proto::Loader;
use HBase::Client::RPC;

sub new {

    my ($class, %args) = @_;

    my $self = bless {

            rpc     => $args{rpc},

        }, $class;

    return $self;
}

sub get_async {

    my ($self, $region_name, $get, $options) = @_;

    my $method = {

            name            => 'Get',

            response_type   => 'HBase::Client::Proto::GetResponse',

        };

    my $request = HBase::Client::Proto::GetRequest->new( {

            region => _region_specifier( $region_name ),

            get    => $get,

        } );

    return $self->_rpc_call_async( $method, $request, $options );

}

sub _locate_region_in_meta {

    my ($self, $region_name) = @_;

    my $request = HBase::Client::Proto::GetRequest->new( {

            region => $self->_region_specifier( 'hbase:meta,,1' ),

            get    => {

                    row              => $region_name,

                    column           => [ { family => 'info' } ],

                    closest_row_before => 1,

                },

        } );

    my $p = $self->_rpc_call_async( { name => 'Get', response_type=>'HBase::Client::Proto::GetResponse' }, $request );

    $p->then( sub { $self->_handle_locate_region_response }, sub {  } );
}

sub _handle_locate_region_response {

    my ($self, $response) = @_;

    my $result = $response->get_result or return;

    my $cells = $result->get_cell_list or return;

    my $cell = first { $_->get_family eq 'info' and  $_->get_qualifier eq 'regioninfo'} @$cells or return;

    my $region_info = HBase::Client::Proto::RegionInfo->decode( substr $cell->get_value, 4 );


}

sub _cells_array_to_map {

    my ($self, $cells) = @_;

    my $rows;

    my %to_sort;

    for my $cell (@$cells){

        my $row = $cell->get_row;

        my $family = $cell->get_family;

        my $qualifier = $cell->get_qualifier;

        my $values = $rows->{$row}->{$family}->{$qualifier} //= [];

        push @$values, $cell;

        $to_sort{\$values} = \$values;

    }

    $$_ = [ sort { $b->get_timestamp <=> $a->get_timestamp } @{$$_} ] for (values %to_sort);

    return $rows;

}

sub _extract_region_location {

    my ($self, $region_name) = @_;

}

sub _rpc_call_async { shift->{rpc}->call_async( @_ ); }

sub _region_specifier {

    my ($self, $region_name) = @_;

    return HBase::Client::Proto::RegionSpecifier->new( {

            type    => HBase::Client::Proto::RegionSpecifier::RegionSpecifierType::REGION_NAME,

            value   => $region_name,

        } );


}

sub _region_name {

    my ($self, $table, $start, $id, $replica_id) = @_;

    $start //= '';

    $id // = '99999999999999';

    my $name = $table . ',' . $start . ',' . $id;

    $name .= '_' . sprintf( '%04X', $replica_id) if $replica_id;

    return $name . '.' . md5_hex( $name );

}

1;