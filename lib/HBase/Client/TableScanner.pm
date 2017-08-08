package HBase::Client::TableScanner;

use v5.14;
use warnings;

use HBase::Client::Utils;
use HBase::Client::Try qw( try retry done handle );

use Promises qw( deferred );

use HBase::Client::Error qw(
        is_exception_error
        is_region_error
    );

sub new {

    my ($class, %args) = @_;

    my $self = bless {
            table               => $args{table},
            scan                => $args{scan},
            number_of_rows      => $args{number_of_rows} // 1000,
            current_start       => $args{scan}->{start_row} // '',
            stop_row            => $args{scan}->{stop_row},
            reversed            => $args{scan}->{reversed},
            next_region         => 0,
            dont_filter_by_region_key_space => $args{dont_filter_by_region_key_space},
        }, $class;

    return $self;

}

sub next {

    my ($self, $options) = @_;

    my $number_of_rows = $options->{number_of_rows} // $self->{number_of_rows};
    my $no_key_filtering = $options->{dont_filter_by_region_key_space} // $self->{dont_filter_by_region_key_space};

    try {

        done(undef) if $self->{completed};

        return $self->_region_scanner
            ->then( sub {

                    my ($scanner) = @_;

                    $self->{completed} = 1 && done(undef) unless $scanner;

                    return $scanner->next( {
                            number_of_rows                  => $number_of_rows + 1, # to have at least one row different from scan start if any left in the region
                            dont_filter_by_region_key_space => $no_key_filtering,
                        } );

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
                        # we drop next_region flag cause on recovery we need to instantiate
                        # the scanner for the region holding the last seen row
                        $self->{current_start} = $results->[-1]->get_cell(0)->get_row;
                        $self->{exclude_start} = 1;
                        $self->{next_region} = 0;

                    }

                    unless (@$results && $response->get_more_results_in_region) {
                        # If there are no (or no more) results in the region, we need to switch to the next one
                        # relatively to the last seen row
                        $self->{next_region} = $self->{reversed} ? -1 : 1;
                        undef $self->{scanner};
                    }

                    if (@$results){

                        return $results;

                    } else {

                        retry( cause => 'Try next region' );

                    }

                }, sub {

                    my ($error) = @_;

                    handle($error);

                    if (is_region_error($error)){

                        undef $self->{scanner};

                        retry( delays => [0.25, 0.5, 1, 2, 4, 8, 10, 10], cause => 'Region error during scan' );

                    } elsif (is_exception_error( $error ) && $error->exception_class eq 'org.apache.hadoop.hbase.UnknownScannerException'){

                        undef $self->{scanner};

                        retry( count => 3, cause => 'Scanner lease expired');

                    } else {

                        warn sprintf( "Error scanning region %s : %s \n", $self->{scanner}->region->name, $error);

                        die $error;

                    }

                } );


    };

}

sub _region_scanner {

    my ($self) = @_;

    # adjusts the scan request to continue from the current start: this is needed to continue interrupted region scanning
    my $row = $self->{scan}->{start_row} = $self->{current_start};

    my ($reversed, $stop_row) = @$self{ qw ( reversed stop_row ) };

    return $self->{scanner} //= $self->{table}->region( $row, $self->{next_region} )->then( sub {
                my ($region) = @_;

                # don't create a scanner if
                # 1) there is no region (like if we try to access a region before the first one of after the last one)
                # 2) stop_row is given and we do normal scan and the region we gonna scan starts after the stop row or at it (cause stop_row is being excluded anyway)
                # 3) stop_row is given and we do reverse scan and the region we gonna scan ends at of before the stop row

                return undef if !region || ($stop_row && ($reversed && $region->end le $stop_row || !$reversed && $region->start ge $stop_row));

                return $region->scanner( $self->{scan}, $self->{number_of_rows} );

            } );

}

1;