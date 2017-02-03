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

    my ($self) = @_;

    my ($node, $scanner_id, $number_of_rows, $next_call_seq) = @$self{ qw ( node scanner_id number_of_rows next_call_seq ) };

    if (defined $scanner_id){

        return $node->_scan_next_async( $scanner_id, $number_of_rows, $next_call_seq )
            ->then( sub {

                    my ($scan_response) = @_;

                    $self->{next_call_seq} = $next_call_seq + 1;

                    return $scan_response;

                } );

    } else {

        my ($region, $scan) = @$self{ qw ( region scan ) };

        return $node->_scan_first_async( $region, $scan, $number_of_rows, $next_call_seq )
            ->then( sub {

                    my ($scan_response) = @_;

                    $self->{next_call_seq} = $next_call_seq + 1;

                    $self->{scanner_id} = $scan_response->get_scanner_id;

                    return $scan_response;

                } );


    }

}

sub close {



}


1;