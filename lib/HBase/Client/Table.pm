package HBase::Client::Table;

use v5.14;
use warnings;

use HBase::Client::Utils;
use HBase::Client::Region;
use HBase::Client::Proto::Loader;
use HBase::Client::TableScanner;
use HBase::Client::Try qw( try retry done handle );
use List::BinarySearch qw( binsearch_pos );
use Promises qw( deferred collect );
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

sub scanner {

    my ($self, $scan, $options) = @_;

    return HBase::Client::TableScanner->new(
            table               => $self,
            scan                => $scan,
            number_of_rows      => $options->{number_of_rows} // 1000,
        );

}

sub get {

    my ($self, $get) = @_;

    return deferred->reject('Getting the closest row before is deprecated since HBase 2.0.0, use reverse scan instead.')->promise if $get->{closest_row_before};

    try {

        return $self->region( $get->{row} )
            ->then( sub {

                    my ($region) = @_;

                    return $region->get_async( $get );
                } )
            ->catch( sub {

                    my ($error) = @_;

                    return $self->handle_error( $error );

                } );

    };

}

sub mutate {

    my ($self, $mutation, $condition, $nonce_group) = @_;

    try {

        return $self->region( $mutation->{row} )
            ->then( sub {
                    my ($region) = @_;

                    return $region->mutate_async( $mutation, $condition, $nonce_group );
                } )
            ->catch( sub {

                    my ($error) = @_;

                    return $self->handle_error( $error );

                } );
    };

}


sub exec_service {

    my ($self, $call) = @_;

    $self->load->then( sub {
        my ($regions) = @_;

        my @promises = map $_->exec_service_async( $request, $call ), @$regions;

        return collect( @promises );

    })->then( sub {

        return \@_;

    });
}


sub handle_error { # TODO

    my ($self, $error) = @_;

    handle($error);

    if (exception($error) eq 'org.apache.hadoop.hbase.NotServingRegionException'
        || exception($error) eq 'org.apache.hadoop.hbase.RegionMovedException'
        || exception($error) eq 'org.apache.hadoop.hbase.regionserver.RegionServerStoppedException'){

        $self->invalidate;

        retry( delays => [0.25, 0.5, 1, 2, 4, 8, 10, 10], cause => exception($error) );

    } else {

        warn exception($error) eq 'unknown' ? $error : exception($error);

        $self->invalidate;

        retry( delays => [0.25, 0.5, 1, 2, 4, 8, 10, 10], cause => exception($error) );

    }

}


sub region {

    my ($self, $row) = @_;

    if ( defined (my $position_in_cache = $self->_region_cache_position_lookup( $row )) ){

        return deferred->resolve( $self->{regions}->[$position_in_cache] )->promise;

    } else {

        $self->load; # start 'async' loading process

        return $self->_region( $row );

    }

}

sub region_after {

    my ($self, $region) = @_;

    return deferred->resolve( undef ) if $region->end eq '';

    return $self->region( $region->end );

}

sub region_before {

    my ($self, $region) = @_;

    return deferred->resolve( undef ) if $region->start eq '';

    if ( defined (my $position_in_cache = $self->_region_cache_position_lookup( $region->start )) ){

        return deferred->resolve( $position_in_cache > 0 ? $self->{regions}->[$position_in_cache-1] : undef )->promise;

    } else {

        $self->load; # start 'async' loading process

        return $self->_region_before( $region );

    }

}

sub invalidate {

    my ($self) = @_;

    $self->{regions} = [];

    return;

}

sub load {

    my ($self) = @_;

    # loading is a continious "async" process whose result we cache to use as a mutex lock
    return $self->{loading} if $self->{loading}; # check the loading process lock

    my $scan = {
            start_row   => region_name( $self->name ),             # "$tablename,,"
            stop_row    => region_name( next_key( $self->name ) ), # "$tablename\x00,,"
        };

    my $scanner = $self->cluster->table( meta_table_name )->scanner( $scan );

    my $regions = $self->{regions} = [];

    # set the loading process lock
    return $self->{loading} = try {

                $scanner->next->then( sub {

                        my ($rows) = @_;

                        if ($rows){

                            for my $row (@$rows){

                                my $region = $self->_region_from_row($row);

                                push @$regions, $region if $region;

                            }

                            retry( cause => 'Check for more regions' );

                        }

                    });
            }
        ->finally( sub {

                undef $self->{loading}; # release the loading process lock

            })
        ->then( sub {

                return $regions;

            }, sub {

                my ($error) = @_;

                $self->handle_error($error);

            } );

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

    my $scan = {
            start_row   => region_name( $self->name, $region->start), # the trick is that any region containing the current region's start row has an id
            reversed    => 1,
        };

    return $self->_scan_for_region( $scan );

}

sub _region {

    my ($self, $row) = @_;

    my $scan = {
            start_row   => region_name( $self->name, $row, '99999999999999'), # nines are to get the most recently open instance of the region
            reversed    => 1,
        };

    return $self->_scan_for_region( $scan );

}

sub _scan_for_region {

    my ($self, $scan) = @_;

    # here we take advantage of the meta-table having a single region that allows us to use unbuffered scanner
    return $self->cluster->table( meta_table_name )->scanner( $scan, {number_of_rows => 1} )->next
        ->then( sub {
                my ($rows) = @_;

                return $rows && @$rows ? $self->_region_from_row( $rows->[0] ) : undef;

            } );

}

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

    if (!$region_info->get_offline && $self->name eq ( $namespace eq 'default' ? '' : $namespace . ':') . $table) {
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
