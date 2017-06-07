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
use HBase::Client::Context qw( context );

sub new {
    my ($class, %args) = @_;

    my $self = bless {
            %args,
            loaded           => undef,
            regions          => [],
            invalidating     => 0,
            version          => 0,
            invalide_regions => {},
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

        my $region;

        return $self->region( $get->{row} )
            ->then( sub {

                    ($region) = @_;

                    return $region->get_async( $get );
                } )
            ->catch( sub {

                    my ($error) = @_;

                    return $self->handle_region_error( $region, $error );

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

                    return $self->handle_region_error( $region, $error );

                } );
    };

}


sub exec_service {

    my ($self, $call) = @_;

    my %results;

    try {

        $self->load->then( sub {

            my $regions = $self->{regions};

            die "No regions for table $self->{name}" unless @$regions;

            my ($retryable_error, $critial_error, @promises);

            for my $region (@$regions){

                unless ( $results{ $region->name } ) {

                    push @promises, $region->exec_service_async( $call )
                        ->then( sub {
                                my ($result) = @_;

                                $results{$region->name} = $result;

                            }, sub {

                                my ($error) = @_;

                                if (exception($error) eq 'org.apache.hadoop.hbase.NotServingRegionException'
                                    || exception($error) eq 'org.apache.hadoop.hbase.regionserver.RegionServerStoppedException'
                                    || exception($error) eq 'org.apache.hadoop.hbase.exceptions.RegionMovedException'){

                                    $retryable_error = $error;

                                    $self->invalidate( $region );

                                } else {

                                    $critial_error = $error;

                                }

                                # eat the error to collect partial results

                            } );

                }

            }

            return collect( @promises )->then( sub {

                    die sprintf( "Coprocessor service error: %s \n", exception($critial_error) eq 'unknown' ? $critial_error : exception($critial_error) ) if $critial_error;

                    retry( count => 6, cause => 'Failed Coprocessor calls') if $retryable_error;

                    return [ @results{ map {$_->name} @$regions } ];

                } );

        });

    }


}


sub handle_region_error { # TODO

    my ($self, $region, $error) = @_;

    handle($error);

    if (exception($error) eq 'org.apache.hadoop.hbase.NotServingRegionException'
        || exception($error) eq 'org.apache.hadoop.hbase.regionserver.RegionServerStoppedException'
        || exception($error) eq 'org.apache.hadoop.hbase.exceptions.RegionMovedException'){

        $self->invalidate( $region );

        retry( delays => [0.25, 0.5, 1, 2, 4, 8, 10, 10], cause => exception($error) );

    } else {

        warn sprintf( "Error querifying a region %s : %s \n", $region->name, exception($error) eq 'unknown' ? $error : exception($error) );

        # the following is desperately irrational:

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

    my ($self, $region) = @_;

    # We assume the caller to be waiting on $table->load lock for the invalidation result

    # There can be multiple execution chains simultaneously discovering that the cache is no longer valid...
    # So we ignore the invalidation request if the table is being invalidated or the region already known to be invlid

    return if $self->{invalidating} || $region && $self->{invalide_regions}->{$region};

    if ($region && $self->{loaded}){

        my $version = $self->{version}; # catch the current table cache version to the closure

        $self->{invalide_regions}->{$region} = 1;

        $self->{loaded} = $self->{loaded}->then( sub {

                    $self->cluster->load_regions( $self->name, $region );

                } )
            ->then( sub {

                    my ($replacement) = @_;

                    return if $self->{invalidating} || $version != $self->{version}; # not interested in the result anymore

                    # do the fix
                    $self->_region_cache_replace( $region, $replacement );

                    return;

                } )
            ->catch( sub {

                    my ($error) = @_;

                    $self->invalidate; # we were not able to fix the cache: drop it

                    die sprintf("Fixing region cache around %s failed cause: %s", $region->name, $error);

                } )
            ->finally( sub {

                    delete $self->{invalide_regions}->{$region};

                } );

    } else {

        $self->load( 1 );

    }

    return;

}

sub inflate {

    my ($self, $regions) = @_;

    $self->{regions} = $regions;

    $self->{loaded} = deferred->resolve($regions)->promise;

    return;

}

sub load {

    my ($self, $force_reload) = @_;

    return $self->{loaded} if $self->{loaded} && !$force_reload;

    context->log( "Loading table $self->{name}..." . ($force_reload ? " reloading" : "") );

    $self->{invalidating} = 1; # blocks invalidation on request; unblocking must be done on the same spin of the event loop as releasing the loading lock

    my $lock;

    return $self->{loaded} = $lock = $self->cluster->load_regions( $self->name )
        ->then( sub {

                my ($regions) = @_;

                if ( $lock == $self->{loaded} ) {

                    $self->{regions} = $regions;
                    $self->{version}++;
                    $self->{invalidating} = 0; # unblocks invalidation on request
                    $self->{invalide_regions} = {};

                    context->log( "Loading table $self->{name} completed" );

                } else {

                    context->log( "Loading table $self->{name} dropped: completed" );

                    die "Loading table $self->{name} dropped";

                }

                return;

            }, sub {

                my ($error) = @_;

                if ( $lock == $self->{loaded} ) {

                    undef $self->{loaded};
                    $self->{invalidating} = 0; # unblocks invalidation on request

                    context->log( "Loading table $self->{name} failed: $error" );

                    die "Loading table $self->{name} failed: $error";

                } else {

                    context->log( "Loading table $self->{name} dropped: failed: $error" );

                    die "Loading table $self->{name} dropped";

                }

            } );
}

GETTERS: {

    no strict 'refs';

    *{$_} = getter( $_ ) for qw( name cluster );

}

sub _region_cache_replace {

    my ($self, $region, $replacement ) = @_;

    if ( defined (my $position = $self->_region_cache_position_lookup( $region->start )) ){

        my $candidate = $self->{regions}->[$position];

        if ($candidate->name eq $region->name){

            splice @{$self->{regions}}, $position, 1, @$replacement;

            return;
        }

    }

    die "Fixing failed";

}

sub _region_cache_position_lookup {

    my ($self, $row) = @_;

    my $regions = $self->{regions} or return undef;

    my $position = binsearch_pos { ($b->start le $a && ($b->end gt $a || $b->end eq '')) ? 0 : $a cmp $b->start } $row, @$regions;

    return $position < @$regions ? $position : undef;

}

1;
