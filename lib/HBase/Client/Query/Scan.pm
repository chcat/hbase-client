package HBase::Client::Query::Scan;

use v5.14;
use warnings;

use parent 'HBase::Client::Query::Abstract';

sub new {

    return shift->SUPER::new( @_, type => 'scan' );

}

sub to_rpc_call {

    my ($self) = @_;

    my ($region, $scan, $scanner_id, $number_of_rows, $next_call_seq, $close_scanner) = @$self{qw (region scan scanner_id number_of_rows next_call_seq close_scanner)};

    my $request = HBase::Client::Proto::ScanRequest->new( {
            defined $scanner_id ? () : (region => $region),
            defined $scanner_id ? () : (scan => $scan),
            defined $scanner_id ? (scanner_id => $scanner_id) : (),
            defined $number_of_rows ? (number_of_rows => $number_of_rows) : (),
            defined $close_scanner ? (close_scanner => $close_scanner) : (),
            next_call_seq => $next_call_seq // 0,
        } );

    return {
            method        => 'Scan',
            response_type => 'HBase::Client::Proto::ScanResponse',
            param         => $request
        };
}

1;