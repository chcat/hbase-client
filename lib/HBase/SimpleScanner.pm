package HBase::SimpleScanner;

use v5.14;
use warnings;

use parent 'HBase::Scanner';

sub new { shift->SUPER::new( @_ ); }

sub next_async {

    my $self = shift;

    my $multi_versions = $_[0]->get_max_versions > 1;

    return $self->SUPER::get_async( @_ )
        ->then( sub {

                my ($response) = @_;

                my $map;

                $self->{client}->transform_cell_array( $_->get_cell, $multi_versions, $map ) for $response->get_results;

                return $map;

            } );

}

1;