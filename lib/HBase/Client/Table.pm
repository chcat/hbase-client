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

use HBase::Client::Error qw(
        is_region_error
        is_exception_error
    );

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

        my $region;

        return $self->region( $mutation->{row} )
            ->then( sub {
                    ($region) = @_;

                    return $region->mutate_async( $mutation, $condition, $nonce_group );
                } )
            ->catch( sub {

                    my ($error) = @_;

                    return $self->handle_region_error( $region, $error );

                } );
    };

}

sub handle_region_error { # TODO

    my ($self, $region, $error) = @_;

    if ($region && is_region_error($error)){

        retry( delays => [0.25, 0.5, 1, 2, 4, 8, 10, 10], cause => $error );

    } else {

        warn sprintf( "Error querifying a region %s : %s \n", $region ? $region->name : 'NO REGION', $error);

        die $error;

    }

}

sub diff {

    my @in;
    my @c;
    my @out;



}


sub exec_service {

    my ($self, $call) = @_;

    my @processed;

    try {

        $self->load->then( sub {

            my @regions = @{ $self->{regions} };

            my (@merged, @to_process);

            my ($i, $j, $segment_ok, $segment_start_i, $segment_start_j) = (0, 0, 1);

            while ($i < @processed && $j < @regions){

                if ( $processed[$i]->{region}->start lt $regions[$j]->start ){

                    $segment_ok &&= !defined $processed[$i]->{error};

                    $i++;

                } elsif ( $processed[$i]->{region}->start gt $regions[$j]->start ){

                    $j++;

                } else {

                    if ($segment_ok){

                        push @merged, @processed[($segment_start_i // 0) .. $i - 1];

                    } else {

                        my @temp = map { { region => $_ } } @regions[ ($segment_start_j // 0) .. $j - 1];

                        push @to_process, @temp;

                        push @merged, @temp;

                    }

                    $segment_start_i = $i;
                    $segment_start_j = $j;

                    $segment_ok = !defined $processed[$i]->{error};

                    $i++;
                    $j++;

                }

            }

            while ($i < @processed) {

                $segment_ok &&= !defined $processed[$i]->{error};

                $i++;

            }

            if ($segment_ok && @processed) {

                push @merged, @processed[($segment_start_i // 0) .. $#processed];

            } else {

                my @temp = map { { region => $_ } } @regions[ ($segment_start_j // 0) .. $#regions];

                push @to_process, @temp;

                push @merged, @temp;

            }

            @processed = @merged;

            my ($fail, $retry, @promises);

            for my $holder (@to_process){

                push @promises, $holder->{region}->exec_service_async( $call )
                    ->then( sub {
                            my ($result) = @_;

                            $holder->{result} = $result;

                        }, sub {

                            my ($error) = @_;

                            $holder->{error} = $error;

                            if (is_region_error($error)){

                                $retry = 1;

                            } else {

                                $fail = 1;

                            }

                        } );

            }

            return collect( @promises )->then( sub {

                    if ($fail) {

                        die [ map { is_exception_error( $_->{error} ) ? () : { error => $_->{error}->exception, region => $_->{region}->name, server => $_->{region}->server } } @processed];

                    } elsif ($retry) {

                        retry( delays => [0.25, 0.5, 1, 2, 4], cause => 'Coprocessor call non fatal failure');

                    } else {

                        return [ map { $_->{result} }  @processed];

                    }

                } );

        });

    }


}

sub region {

    my ($self, $row) = @_;

    return $self->load->then( sub {

            if ( defined (my $position_in_cache = $self->_region_cache_position_lookup( $row )) ){

                return $self->{regions}->[$position_in_cache];

            } else {

                die sprintf("Table %s cache currupted: was looking for a region for %s \n", $self->name, $row);

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

                die sprintf("Table %s cache is currupted: was looking for a region before %s \n", $self->name, $region);

            }

        } );

}

sub invalidate {

    my ($self, $region) = @_;

    return $self->cluster->load_regions( $self->name, $region->start, $region->end )
            ->then( sub {

                    my ($regions) = @_;

                    $self->_patch_cache( $regions );

                    return;

                } )
            ->catch( sub {

                    my ($error) = @_;

                    warn sprintf("Fixing region cache around %s failed cause: %s", $region->name, $error);

                    die $error;

                } );

}

sub load {

    my ($self, $regions) = @_;

    if ($regions) {

        $self->_inflate( $regions );

        return $self->{loaded} = deferred->resolve;

    }

    return $self->{loaded} //= $self->cluster->load_regions( $self->name )
        ->then( sub {

                my ($regions) = @_;

                die 'No regions present' unless @$regions;

                $self->_inflate( $regions );

                return;

            })
        ->catch( sub {

                my ($error) = @_;

                undef $self->{loaded};

                die sprintf("Loading table %s failed: %s \n", $self->name, $error);

            } );
}

GETTERS: {

    no strict 'refs';

    *{$_} = getter( $_ ) for qw( name cluster );

}

sub _inflate {

    my ($self, $regions) = @_;

    $regions = $self->_fix_region_sequence( $regions );

    if (@$regions) {

        unshift @$regions, HBase::Client::Region->dummy( table_name => $self->name, start => '', end => $regions->[0]->start ) if $regions->[0]->start ne '';

        push @$regions, HBase::Client::Region->dummy( table_name => $self->name, start => $regions->[-1]->end, end => '' ) if $regions->[-1]->end ne '';

    } else {

        push @$regions, HBase::Client::Region->dummy( table_name => $self->name, start => '', end => '' );

    }

    $self->{regions} = $regions;

    return;

}

# The main thing is to have a "partition" (in math sence) of the key space onto regions after patching the cache assuming
# it was correct (=partition) before.
sub _patch_cache {

    my ($self, $patch) = @_;

    die 'Empty region map patch' unless ($patch && @$patch);

    $patch = $self->_fix_region_sequence( $patch );

    die 'Bad region map patch' unless (@$patch);

    my $start = $patch->[0]->start;
    my $end = $patch->[-1]->end;

    my $start_position = $self->_region_cache_position_lookup( $start );

    die 'Broken table cache' unless defined $start_position;

    my $outdated_first = $self->{regions}->[$start_position];

    unshift @$patch, HBase::Client::Region->dummy( table_name => $self->name, start => $outdated_first->start, end => $start ) if $outdated_first->start ne $start;

    my $end_position = $start_position;

    my $outdated_last = $self->{regions}->[$end_position];

    while ($outdated_last->end ne '' && ($outdated_last->end lt $end || $end eq '' )  ) {

        $outdated_last = $self->{regions}->[++$end_position];

    }

    push @$patch, HBase::Client::Region->dummy( table_name => $self->name, start => $end, end => $outdated_last->end ) if $outdated_last->end ne $end;

    splice @{$self->{regions}}, $start_position, $end_position - $start_position + 1, @$patch;

}

# A "good" sequence of regions is the one that forms a coverage of some interval of keys.
# The sad truth is that the regions present in the meta usually does not form a "good" sequence.
# Here we try to fix the given sequence.
sub _fix_region_sequence {

    my ($self, $regions) = @_;

    my @fixed;

    for my $region (@$regions){

        next if $region->table_name ne $self->name || $region->is_invalid;

        my $previous_region = $fixed[-1];

        while ($previous_region && ( $region->start lt $previous_region->end || $previous_region->end eq '')) {

            # well, looks like the meta is completely broken :(

            warn sprintf( "Bad region sequence: %s ends at %s while %s in online \n", $previous_region->name, $previous_region->end, $region->name );

            pop @fixed;

            $previous_region = $fixed[-1];

        }

        # at this point, the previous region, if any, ends at or before the current start

        if ($previous_region){

            if ($previous_region->end eq $region->start) {

                push @fixed, $region;

            } elsif ($previous_region->end lt $region->start){

                # there is a hole between regions :(

                warn sprintf( "Bad region sequence: %s ends at %s while the next is %s \n", $previous_region->name, $previous_region->end, $region->name );

                # we fill the hole in the region map by a dummy region

                push @fixed, HBase::Client::Region->dummy( table_name => $self->name, start => $previous_region->end, end => $region->start ), $region;

            }

        } else {

            push @fixed, $region;

        }

    }

    return \@fixed;

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
