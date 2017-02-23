package HBase::SimpleScanner;

use v5.14;
use warnings;

use parent 'HBase::Scanner';

sub new {

    my ($class, %args) = @_;

    my $self = $class->SUPER::new( %args );

    $self->{multi_versions} = $args{scan}->{max_versions} // 1 > 1;

    return $self;
}

sub next_async {

    my $self = shift;

    return $self->SUPER::next_async( @_ )
        ->then( sub {

                my ($response) = @_;

                return undef unless $response and $response->has_results_list;

                my $rows = {};

                $self->{client}->_transform_cell_array( $_->get_cell_list, $self->{multi_versions}, $rows ) for @{$response->get_results_list};

                return $rows;

            } );

}

1;