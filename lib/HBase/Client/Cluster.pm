package HBase::Client::Cluster;

use v5.14;
use warnings;

use HBase::Client::Proto::Loader;
use HBase::Client::Utils;

sub new {

    my ($class, %args) = @_;

    my $meta_holder_locator = $args{meta_holder_locator};

    my $node_pool = $args{node_pool};

    my $self = bless {
            meta_holder_locator => $meta_holder_locator,
            node_pool           => $node_pool,
        }, $class;

    return $self;

}

sub get_async {

    my ($self, $table, $get) = @_;

    return $self->_get_region_and_node( $table, $get->{row} )->then( sub {

                my ($region, $node) = @_;

                return $node->get_async( $region, $get );

            } );

}

sub mutate_async {

    my ($self, $table, $mutation, $condition, $nonce_group) = @_;

    return $self->_get_region_and_node( $table, $mutation->{row} )->then( sub {

                my ($region, $node) = @_;

                return $node->mutate_async( $region, $mutation, $condition, $nonce_group );

            } );
}

sub _scan_first_async {

    my ($self, $table, $scan, $number_of_rows) = @_;

    my $row = $scan->{start_row} // '';

    return $self->_get_region_and_node( $table, $row )
        ->then( sub {

                my ($region, $node) = @_;

                return $node->scan_first_async( $region, $scan, $number_of_rows );

            } );

}

sub _get_region_and_node {

     my ($self, $table, $row) = @_;

     if ($table eq meta_table_name) {

        return $self->_locate_meta_holder
            ->then( sub {

                    my ($server) = @_;

                    return $self->_get_node( $server );

                } )
            ->then( sub { return (meta_region_specifier, @_) } } );

     }

     my $region;

     return $self->_locate_region( $table, $row )
        ->then( sub {

                my ($location) = @_;

                $region = region_specifier( $location->{region_name} );

                return $self->_get_node( $location->{server} );

            } )
        ->then( sub { return ($region, @_) } );

}

sub _get_node {

    my ($self, $server) = @_;

    return $self->{node_pool}->get_node( $server );

}

sub _locate_region {

    my ($self, $table, $row) = @_;

    return $self->_locate_meta_holder
        ->then( sub {

                my ($server) = @_;

                return $self->_get_node( $server );

            } )
        ->then( sub {

                my ($node) = @_;

                return $self->_query_meta( $node, $table, $row );

            } )
        ->then( sub {

                my ($response) = @_;

                return $self->_handle_query_meta_response( $response );

            } );

}

sub _locate_meta_holder {

    my ($self) = @_;

    return $self->{meta_holder} //= $self->{meta_holder_locator}->locate;

}

sub _query_meta {

    my ($self, $node, $table, $row) = @_;

    my $get = {
            row              => region_name( $table, $row, '99999999999999'),
            column           => [ { family => 'info' } ],
            closest_row_before => 1,
        };

    return $node->get_async( meta_region_specifier, $get );

}

sub _handle_query_meta_response {

    my ($self, $response) = @_;

    my $result = $response->get_result or die;

    my $rows = cell_array_to_row_map( $result->get_cell_list );

    my ($region_name) = keys %$rows or die;

    my $row = $rows->{$region_name};

    my $region_info_encoded = $row->{info}->{regioninfo}->[0]->{value} // die;

    my $region_info = HBase::Client::Proto::RegionInfo->decode( substr $region_info_encoded, 4 );

    return {
            region_name        => $region_name,
            server      => $row->{info}->{server}->[0]->{value},
            start       => $region_info->get_start_key,
            end         => $region_info->get_end_key,
        };

}


1;