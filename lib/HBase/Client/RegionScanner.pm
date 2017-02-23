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

sub next_async {

    my ($self, $exclude_start) = @_;

    my ($node, $scanner_id, $number_of_rows, $next_call_seq) = @$self{ qw ( node scanner_id number_of_rows next_call_seq ) };
    my ($region, $scan) = defined $scanner_id ? () : @$self{ qw ( region scan ) };

    return $node->scan_async( $region, $scan, $scanner_id, $number_of_rows + !!$exclude_start, $next_call_seq)
        ->then( sub {

                my ($response) = @_;

                $self->{next_call_seq} = $next_call_seq + 1;
                $self->{scanner_id} = $response->get_scanner_id unless defined $scanner_id;

                if ($exclude_start and my @results = @{$response->get_results_list}){
                    if ($results[0]->get_cell(0)->get_row eq $scan->{start_row}){
                        shift @results;
                        $response->set_results_list( [@results] );
                    }
                }

                return $response;

            } );


}

sub close {

    #TODO send close signal to release the scanner

}


1;