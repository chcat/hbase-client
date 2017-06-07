package HBase::Client::TableScanner;

use v5.14;
use warnings;

use HBase::Client::Utils;
use HBase::Client::Try qw( try retry done handle );

use Promises qw( deferred );

sub new {

    my ($class, %args) = @_;

    my $self = bless {
            table               => $args{table},
            scan                => $args{scan},
            number_of_rows      => $args{number_of_rows} // 1000,
            current_start       => $args{scan}->{start_row} // '',
            stop_row            => $args{scan}->{stop_row},
            reversed            => $args{scan}->{reversed},
        }, $class;

    return $self;

}

sub next {

    my ($self, $options) = @_;

    my $number_of_rows = $options->{number_of_rows} // $self->{number_of_rows};

    try {

        done(undef) if $self->{completed};

        my $scanner = $self->{scanner} //= $self->_new_region_scanner;

        return $scanner
            ->then( sub {

                    my ($scanner) = @_;

                    $self->{completed} = 1 && done(undef) unless $scanner;

                    return $scanner->next( {number_of_rows => $number_of_rows} );

                } )
            ->then( sub {

                    my ($response) = @_;

                    my $results = $response->get_results_list // [];

                    if ($self->{exclude_start} && @$results){
                        if ($results->[0]->get_cell(0)->get_row eq $self->{current_start}){
                            shift @$results;
                        }
                    }

                    if (@$results){

                        # update the last seen row to be able to recover after scanner loss
                        $self->{current_start} = $results->[-1]->get_cell(0)->get_row;
                        $self->{exclude_start} = 1,

                        return $results;

                    } else {
                        # no rows left in the region?
                        warn 'Empty scan result yet more results in the region'
                            and retry( count => 1, cause => 'More rows expected' ) if $response->get_more_results_in_region;

                        undef $self->{scanner};

                        $self->_pick_next_region;

                        retry;

                    }

                }, sub {

                    my ($error) = @_;

                    handle($error);

                    if (exception($error) eq 'org.apache.hadoop.hbase.UnknownScannerException' ){
                        # most likely we have our scanner timed out, so we retry requesting a new one
                        undef $self->{scanner};

                        retry( count => 3, cause => 'Got UnknownScannerException - most likely scanner timed out');

                    } elsif (exception($error) eq 'org.apache.hadoop.hbase.NotServingRegionException'
                        || exception($error) eq 'org.apache.hadoop.hbase.RegionMovedException'
                        || exception($error) eq 'org.apache.hadoop.hbase.regionserver.RegionServerStoppedException'
                        || exception($error) eq 'org.apache.hadoop.hbase.exceptions.RegionMovedException'){


                        undef $self->{scanner};

                        $self->{table}->invalidate;

                        retry( delays => [0.25, 0.5, 1, 2, 4, 8, 10, 10], cause => exception($error) );

                    } else {

                        warn exception($error) eq 'unknown' ? $error : exception($error);

                        undef $self->{scanner};

                        $self->{table}->invalidate;

                        retry( delays => [0.25, 0.5, 1, 2, 4, 8, 10, 10], cause => exception($error) );

                    }

                    die $error;

                } );


    };

}

sub _new_region_scanner {

    my ($self) = @_;

    # adjusts the scan request to continue from the current start: this is needed to continue interrupted region scanning
    $self->{scan}->{start_row} = $self->{current_start};

    return $self->_region
        ->then( sub {
                my ($region) = @_;

                return undef unless $region;

                return $region->scanner( $self->{scan}, $self->{number_of_rows}, $self->{exclude_start} );

            } );

}

# returns a promise of the region to scan
sub _region {

    my ($self) = @_;

    return $self->{region} //= $self->{table}->region( $self->{current_start} );

}

# updates a promise of the region to scan with the next one
sub _pick_next_region {

    my ($self) = @_;

    my ($reversed, $stop_row) = @$self{ qw ( reversed stop_row ) };

    $self->{region} = $self->_region
        ->then( sub {

                my ($region) = @_;

                return undef if $stop_row && ($reversed && $region->start le $stop_row || !$reversed && $region->end gt $stop_row);

                return $reversed ? $self->{table}->region_before( $region ) : $self->{table}->region_after( $region );

            } );

    return 1;

}


1;