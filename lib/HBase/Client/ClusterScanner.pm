package HBase::Client::ClusterScanner;

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





}

sub _scan_first_async {

    my ($self, $table, $scan, $number_of_rows) = @_;

    my $row = $scan->{start_row} // '';

    return $self->_get_region_and_node( $table, $row )
        ->then( sub {

                my ($region, $node) = @_;

                return $node->scan_first_async( $region, $scan, $number_of_rows );

            } );

}


1;