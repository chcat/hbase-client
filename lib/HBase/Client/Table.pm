package HBase::Client::Table;

use v5.14;
use warnings;

use HBase::Client::Utils;
use HBase::Client::Try;
use List::BinarySearch qw( binsearch_pos );
use Promises qw( deferred );
use Scalar::Util qw( weaken );

sub new {
    my ($class, %args) = @_;

    my $self = bless {
            %args,
            regions => [],
        }, $class;

    weaken $self->{cluster};

    return $self;

}

sub region {

    my ($self, $row) = @_;

    if ( defined my $position_in_cache = $self->_region_cache_position_lookup( $row ) ){

        return deferred->resolve( $regions->[$position_in_cache] )->promise;

    } else {

        $self->_load; # start 'async' loading process

        return $self->_region( $row );

    }

}

sub region_after {

    my ($self, $region) = @_;

    return deferred->resolve( undef ) if $region->end eq '';

    return $self->get_region( $region->end );

}

sub region_before {

    my ($self, $region) = @_;

    return deferred->resolve( undef ) if $region->start eq '';

    if ( defined my $position_in_cache = $self->_region_cache_position_lookup( $region->start ) ){

        return deferred->resolve( $position_in_cache > 0 ? $regions->[$position_in_cache-1] : undef )->promise;

    } else {

        $self->_load; # start 'async' loading process

        return $self->_region_before( $region );

    }

}

sub load {

    my ($self) = @_;

    # loading is a continious "async" process whose result we cache to use as a mutex lock
    return $self->{loading} if $self->{loading}; # check the loading process lock

    my $scan = {
            start_row   => region_name( $self->name ),             # "$tablename,,"
            stop_row    => region_name( next_key( $self->name ) ), # "$tablename\x00,,"
        };

    my $cluster = $self->cluster;

    my $scanner = $cluster->scanner( meta_table_name, $scan, 1000 );

    my $regions = $self->{regions} = [];

    # set the loading process lock
    return $self->{loading} = try {

        $scanner->next->then( sub {

                my ($response) = @_;

                if ($response){

                    push @$regions, $cluster->region_from_row($_) for @{$response->get_results_list // []};

                    retry( cause => 'Check for more regions' );

                } else {

                    undef $self->{loading}; # release the loading process lock

                }

            });

    }

}

GETTERS: {

    sub _getter {

        my ($property) = @_;

        return sub { $_[0]->{$property} };

    }

    no strict 'refs';

    *{$_} = _getter( $_ ) for qw( name cluster );

}

sub _region_cache_position_lookup {

    my ($self, $row) = @_;

    my $regions = $self->{regions};

    my $position = binsearch_pos { ($b->start le $a && ($b->end gt $a || $b->end eq '')) ? 0 : $a cmp $b->start } $row, @$regions;

    return $position < @$regions ? $position : undef;

}

sub _region_before {

    my ($self, $region) = @_;

    return $self->get_meta_region
        ->then( sub {
                my ($region) = @_;

                my $scan = {
                        start_row   => $region->name,
                        reversed    => 1,
                    };

                return $region->scanner( $scan, 2, 1 )->next;
            } )
        ->then( sub {

                my ($response) = @_;

                return undef unless $response->results_size;

                my $region = $self->region_from_row( $response->get_results(0) );

                return $region && $region->table eq $table ? $region : undef;

            } );

}

sub _region {

    my ($self, $row) = @_;

    return $self->_meta_region
        ->then( sub {
                my ($region) = @_;

                my $scan = {
                        start_row   => region_name( $self->name, $row, '99999999999999'),
                        reversed    => 1,
                    };

                return $region->scanner( $scan, 1 )->next;
            } )
        ->then( sub {

                my ($response) = @_;

                return $response->results_size ? $self->_region_from_row( $response->get_results(0) ) : undef;

            } );

}

sub _meta_region { shift->cluster->get_meta_region }

sub _region_from_row {

    my ($self, $result) = @_;

    my $rows = cell_array_to_row_map( $result->get_cell_list );

    my ($region_name) = keys %$rows or return undef;

    my $row = $rows->{$region_name};

    my $region_info_encoded = $row->{info}->{regioninfo}->[0]->{value} // die;

    my $region_info = HBase::Client::Proto::RegionInfo->decode( substr $region_info_encoded, 4 );

    my $table_name = $region_info->get_table_name;

    my $namespace = $table_name->get_namespace;

    my $table = $table_name->get_qualifier;

    if ( $self->name eq ( $namespace eq 'default' ? '' : $namespace . ':') . $table) {
        return HBase::Client::Region->new(
                name        => $region_name,
                server      => $row->{info}->{server}->[0]->{value},
                start       => $region_info->get_start_key,
                end         => $region_info->get_end_key,
                table       => $self,
            );
    } else {

        return undef;

    }

}

1;