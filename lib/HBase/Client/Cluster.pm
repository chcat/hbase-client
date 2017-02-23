package HBase::Client::Cluster;

use v5.14;
use warnings;

use HBase::Client::TableScanner;
use HBase::Client::Proto::Loader;
use HBase::Client::Utils;
use HBase::Client::Try;
use HBase::Client::Region;

use Promises qw( deferred );

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

    try {

        return $self->get_region( $table, $get->{row} )
            ->then( sub {
                    my ($region) = @_;

                    return $region->get_async( $get );
                } )
            ->catch( sub {

                    my ($error) = @_;

                    if (exception($error) eq 'org.apache.hadoop.hbase.NotServingRegionException' ){

                        retry( count => 3, cause => $error );

                    } else {

                        die $error;

                    }

                } );

    };

}

sub mutate_async {

    my ($self, $table, $mutation, $condition, $nonce_group) = @_;

    try {

        return $self->_get_region( $table, $mutation->{row} )
            ->then( sub {
                    my ($region) = @_;

                    return $region->mutate_async( $mutation, $condition, $nonce_group );
                } )
            ->catch( sub {

                    my ($error) = @_;

                    if (exception($error) eq 'org.apache.hadoop.hbase.NotServingRegionException' ){

                        retry( count => 3, cause => $error );

                    } else {

                        die $error;

                    }

                } );
    };

}

sub scanner {

    my ($self, $table, $scan, $number_of_rows) = @_;

    return HBase::Client::TableScanner->new(
            cluster             => $self,
            table               => $table,
            scan                => $scan,
            number_of_rows      => $number_of_rows,
        );

}

sub get_region_before {

    my ($self, $region) = @_;

    my $table = $region->table;

    return deferred->resolve(undef)->promise if $table eq meta_table_name or $region->start eq '';

    my $scan = {
            start_row   => $region->name,
            reversed    => 1,
        };

    return get_meta_region->scanner( $scan, 2 )->next_async( 1 )
        ->then( sub{

                my ($response) = @_;

                return undef unless $response->results_size;

                my $region = $self->_parse_region_from_row( $response->get_results(0) );

                return $region && $region->table eq $table ? $region : undef;

            } );

}

sub get_region_after {

    my ($self, $region) = @_;

    my $table = $region->table;

    return deferred->resolve(undef)->promise if $table eq meta_table_name or $region->end eq '';

    return get_region( $table, $region->end );

}

sub get_region {

    my ($self, $table, $row) = @_;

    my $meta_region = $self->get_meta_region;

    return $meta_region if $table eq meta_table_name;

    my $get = {
            row              => region_name( $table, $row, '99999999999999'),
            column           => [ { family => 'info' } ],
            closest_row_before => 1,
        };

    return $meta_region->get_async( $get )
        ->then( sub {
                my ($response) = @_;

                my $region = $self->_parse_region_from_row( $response->get_result );
                # filter out regions of other tables for the case the target table does not exists
                return $region && $region->table eq $table ? $region : undef;
            } );
}

sub get_meta_region {

    my ($self) = @_;

    return $self->_meta_holder
        ->then( sub {

                 my ($server) = @_;

                 return HBase::Client::Region->new(
                     name        => 'hbase:meta,,1',
                     server      => $server,
                     start       => '',
                     end         => '',
                     cluster     => $self,
                     table       => meta_table_name,
                 );

            } );

}

sub get_node {

    my ($self, $server) = @_;

    return $self->{node_pool}->get_node( $server );

}

sub _meta_holder {

    my ($self) = @_;

    return $self->{meta_holder} //= $self->_locate_meta_holder;

}

sub _locate_meta_holder {

    my ($self) = @_;

    try {
        return $self->{meta_holder_locator}->locate
            ->catch( sub {

                    retry(count => 3, cause => $_[0]);

                } );
    };

}

sub _parse_region_from_row {

    my ($self, $result) = @_;

    my $rows = cell_array_to_row_map( $result->get_cell_list );

    my ($region_name) = keys %$rows or return undef;

    my $row = $rows->{$region_name};

    my $region_info_encoded = $row->{info}->{regioninfo}->[0]->{value} // die;

    my $region_info = HBase::Client::Proto::RegionInfo->decode( substr $region_info_encoded, 4 );

    my $table_name = $region_info->get_table_name;

    my $namespace = $table_name->get_namespace;

    my $table = $table_name->get_qualifier;

    return HBase::Client::Region->new(
            name        => $region_name,
            server      => $row->{info}->{server}->[0]->{value},
            start       => $region_info->get_start_key,
            end         => $region_info->get_end_key,
            cluster     => $self,
            table       => ( $namespace eq 'default' ? '' : $namespace . ':') . $table ,
        );

}


1;