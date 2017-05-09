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

        my @promises = map $_->exec_service_async( $call ), @$regions;

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
        || exception($error) eq 'org.apache.hadoop.hbase.regionserver.RegionServerStoppedException'
        || exception($error) eq 'org.apache.hadoop.hbase.exceptions.RegionMovedException'){

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

    return $self->load->then( sub {

            if ( defined (my $position_in_cache = $self->_region_cache_position_lookup( $row )) ){

                return $self->{regions}->[$position_in_cache];

            } else {

                # Table loaded successfully, but there is no region for the row? Either we corrupted
                # the region cache somehow or the table does not exists...

                $self->invalidate;

                die "No regions are present for table $self->{name}: probably the table does not exist";

            }

        } );

}

sub region_after {

    my ($self, $region) = @_;

    return deferred->resolve( undef ) if $region->end eq '';

    return $self->region( $region->end );

}

sub region_before {

    my ($self, $region) = @_;

    return deferred->resolve( undef ) if $region->start eq '';

    return $self->load->then( sub {

            if ( defined (my $position_in_cache = $self->_region_cache_position_lookup( $region->start )) ){

                return $position_in_cache > 0 ? $self->{regions}->[$position_in_cache-1] : undef;

            } else {

                # Table loaded successfully, but there is no region for the row? Either we corrupted
                # the region cache somehow or the table does not exists...

                $self->invalidate;

                die "No regions are present for table $self->{name}: probably the table does not exist";

            }

        } );

}

sub invalidate {

    my ($self) = @_;

    undef $self->{loaded}; # release the loading process lock

    return;

}



sub load {

    my ($self, $regions) = @_;

    if ($regions) {

        $self->{regions} = $regions;

        return $self->{loaded} = deferred->resolve($regions)->promise;

    }

    return $self->{loaded} //= $self->cluster->load_regions( $self->name )
        ->then( sub {

                my ($regions) = @_;

                $self->{regions} = $regions;

                return $regions;

            }, sub {

                my ($error) = @_;

                $self->invalidate; # release the loading process lock and drop the cache

                die "Loading table $self->{name} failed: $error";

            } );
}

GETTERS: {

    no strict 'refs';

    *{$_} = getter( $_ ) for qw( name cluster );

}

sub _region_cache_position_lookup {

    my ($self, $row) = @_;

    my $regions = $self->{regions};

    my $position = binsearch_pos { ($b->start le $a && ($b->end gt $a || $b->end eq '')) ? 0 : $a cmp $b->start } $row, @$regions;

    return $position < @$regions ? $position : undef;

}

1;
