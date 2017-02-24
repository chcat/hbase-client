package HBase::Client::TableScanner;

use v5.14;
use warnings;

use HBase::Client::Utils;
use HBase::Client::Try;

use Promises qw( deferred );

sub new {

    my ($class, %args) = @_;

    my $self = bless {
            %args,
            next_call_seq       => 0,
            current_start       => $args{scan}->{start_row} // '',
            stop_row            => $args{scan}->{stop_row},
            reversed            => $args{scan}->{reversed},
        }, $class;

    return $self;

}

sub next_async {

    my ($self) = @_;

    return deferred->resolve(undef)->promise if $self->{completed};

    try {

        my $scanner = $self->{scanner} //= $self->_new_region_scanner;

        return $scanner
            ->then( sub {

                    my ($scanner) = @_;

                    $self->{completed} = 1 && return undef unless $scanner;

                    return $scanner->next_async;

                } )
            ->then( sub {

                    my ($response) = @_;

                    if (my @results = @{$response->get_results_list}){
                        # update the last seen row to be able to recover after scanner loss
                        $self->{current_start} = $results[-1]->get_cell(0)->get_row;
                        $self->{exclude_start} = 1,

                        return $response;

                    } else {
                        # no rows left in the region?
                        warn 'Empty scan result yet more results in the region'
                            and retry( count => 1, cause => 'More rows expected' ) if $response->get_more_results_in_region;

                        $self->_pick_next_region;

                        retry;

                    }

                }, sub {

                    my ($error) = @_;

                    if (exception($error) eq 'org.apache.hadoop.hbase.UnknownScannerException' ){
                        # most likely we have our scanner timed out, so we retry requesting a new one
                        undef $self->{scanner};

                        retry( count => 3, cause => $error );
                    } else {

                        die $error;

                    }

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

    return $self->{region} //= $self->{cluster}->get_region( $self->{table}, $self->{current_start} );

}

# updates a promise of the region to scan with the next one
sub _pick_next_region {

    my ($self) = @_;

    my ($reversed, $stop_row) = @$self{ qw ( reversed stop_row ) };

    $self->{region} = $self->_region
        ->then( sub {

                my ($region) = @_;

                return undef if $reversed && $region->start le $stop_row || !$reversed && $region->end gt $stop_row;

                return $reversed ? $region->region_before : $region->region_after;

            } );

    return 1;

}


1;