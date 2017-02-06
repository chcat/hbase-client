package HBase::Client::ClusterScanner;

use v5.14;
use warnings;

sub new {

    my ($class, %args) = @_;

    my $self = bless {
            %args,
            next_call_seq => 0,
            start_row     => $args{scan}->{start_row} // '',
            stop_row      => $args{scan}->{stop_row} // '',
        }, $class;

    return $self;

}

sub next_async {

    my ($self) = @_;

    my $scanner = $self->{scanner} //= $self->_get_region_scanner;

    return $scanner->then( sub { return shift->next_async; } );

}

sub _get_region_scanner {

    my ($self) = @_;

    return $self->_get_region_and_node( $self->{table}, $self->{start_row} )
        ->then( sub {

                my ($region, $node) = @_;

                return $node->scan( $region, $scan, $number_of_rows );

            } );

}


1;