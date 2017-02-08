package HBase::SimpleScanner;

use v5.14;
use warnings;

use parent 'HBase::Scanner';

sub new {

    my ($class, %args) = @_;

    my $self = $class->SUPER::new( %args );

    $self->{multi_versions} = $args{scan}->{max_versions} > 1;

    return $self;
}

sub next_async {

    my $self = shift;

    return $self->SUPER::next_async( @_ )
        ->then( sub {

                my ($response) = @_;

                my $map;

                $self->{client}->transform_cell_array( $_->get_cell, $self->{multi_versions}, $map ) for @{$response->get_results_list};

                return $map;

            } );

}

1;