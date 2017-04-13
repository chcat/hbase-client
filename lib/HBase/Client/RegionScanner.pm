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

                return $response;

            } );


}

sub close {

    #TODO send close signal to release the scanner

}


1;