package HBase::Client::Node;

use v5.14;
use warnings;

use HBase::Client::Proto::Loader;
use HBase::Client::RegionScanner;

use constant GET => { name => 'Get', response_type=>'HBase::Client::Proto::GetResponse' };
use constant MUTATE => { name => 'Mutate', response_type=>'HBase::Client::Proto::MutateResponse' };
use constant SCAN => { name => 'Scan', response_type=>'HBase::Client::Proto::ScanResponse' };

sub new {

    my ($class, %args) = @_;

    my $self = bless {
            rpc     => $args{rpc},
        }, $class;

    return $self;
}

sub connect {

    my ($self, $cb ) = @_;

    $cb->();

}

sub get_async {

    my ($self, $region, $get) = @_;

    my $request = HBase::Client::Proto::GetRequest->new( {
            region => $region,
            get    => $get,
        } );

    return $self->_rpc_call_async( GET, $request );

}

sub mutate_async {

    my ($self, $region, $mutation, $condition, $nonce_group) = @_;


    my $request = HBase::Client::Proto::MutateRequest->new( {
            region      => $region,
            mutation    => $mutation,
            $condition ? (condition => $condition) : (),
            defined $nonce_group ? (nonce_group => $nonce_group) : (),
        } );

    return $self->_rpc_call_async( MUTATE, $request );

}

sub scan {

    my ($self, $region, $scan, $number_of_rows) = @_;

    return HBase::Client::RegionScanner->new(
            node                => $self,
            region              => $region,
            scan                => $scan,
            number_of_rows      => $number_of_rows,
        );

}

sub _scan {

    my ($self, $region, $scan, $scanner_id, $number_of_rows, $next_call_seq, $close_scanner) = @_;

    my $request = HBase::Client::Proto::ScanRequest->new( {
            $region ? (region => $region) : (),
            $scan ? (scan => $scan) : (),
            defined $scanner_id ? (scanner_id => $scanner_id) : (),
            defined $number_of_rows ? (number_of_rows => $number_of_rows) : (),
            defined $close_scanner ? (close_scanner => $close_scanner) : (),
            defined $next_call_seq ? (next_call_seq => $next_call_seq) : (),
        } );

    return $self->_rpc_call_async( SCAN, $request );

}

sub _rpc_call_async { shift->{rpc}->call_async( @_ ); }

1;