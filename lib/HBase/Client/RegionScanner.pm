package HBase::Client::RegionScanner;

use v5.14;
use warnings;

sub new {

    my ($class, %args) = @_;

    my $self = bless {
            %args,
            next_call_seq => 0,
        }, $class;

    return $self;

}

sub next {

    my ($self, $options) = @_;

    my ($region, $scan, $scanner_id, $next_call_seq) = @$self{ qw ( region scan scanner_id next_call_seq ) };

    my $number_of_rows = $options->{number_of_rows} // $self->{number_of_rows};

    return $region->scan_async( $scan, $scanner_id, $number_of_rows, $next_call_seq)
        ->then( sub {

                my ($response) = @_;

                $self->{next_call_seq} = $next_call_seq + 1;
                $self->{scanner_id} = $response->get_scanner_id unless defined $scanner_id;

                # Under certain conditions, region's files could contain information about rows
                # that do no belong to its key space: for example, right after split. This also
                # could happen as a result of the cluster being in an inconsistent state.

                # So, by default, we filter out the rows by key space

                unless ($options->{dont_filter_by_region_key_space}){

                    my $results = $response->get_results_list // [];

                    my @filtered_results = map {
                            my $row = $_->get_cell(0)->get_row;
                            $row ge $region->start && ( $row lt $region->end || $region->end eq '') ? $_ : ();
                        } @$results;

                    warn sprintf( "Region %s contains data beyond its end\n", $region->name ) if @filtered_results != @$results;

                    $response->set_results_list( \@filtered_results );

                }

                return $response;

            } );


}

sub region { $_[0]->{region}; }

sub close {

    #TODO send close signal to release the server-side scanner

}


1;