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

    my ($region, $scan) = defined $scanner_id ? () : @$self{ qw ( region scan ) };

    return $node->_scan( $region, $scan, $scanner_id, $number_of_rows, $next_call_seq)
        ->then( sub {

                my ($scan_response) = @_;

                $self->{next_call_seq} = $next_call_seq + 1;

                $self->{scanner_id} = $scan_response->get_scanner_id unless defined $scanner_id;

                return $scan_response;

            } );


}

sub close {



}


1;