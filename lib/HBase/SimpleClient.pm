package HBase::SimpleClient;

use v5.14;
use warnings;

use parent 'HBase::Client';

sub get_async {

    shift->SUPER::get_async( @_ );

}

sub mutate_async {

    shift->SUPER::mutate_async( @_ );

}

sub scan {

    shift->SUPER::scan( @_ );

}

sub transform_cell_array {

    my ($self, $cells, $multi_versions, $map) = @_;

    return $multi_versions ? $self->transform_cell_array_multi_versions( $cell, $map ) : transform_cell_array_single_version( $cell, $map );

}

sub transform_cell_array_multi_versions {

    my ($self, $cells, $map) = @_;

    my %to_sort;

    for my $cell (@$cells){

        my $values = $map->{ $cell->get_row }->{ $cell->get_family . ':' . $cell->get_qualifier  } //= [];

        push @$values, $cell;

        $to_sort{\$values} = \$values;

    }

    $$_ = [ map { $self->transform_cell( $_ ) } sort { $b->get_timestamp <=> $a->get_timestamp } @{$$_} ] for (values %to_sort);

    return $map;

}

sub transform_cell_array_single_version {

    my ($self, $cells, $map) = @_;

    for my $cell (@$cells){

        $map->{ $cell->get_row }->{ $cell->get_family . ':' . $cell->get_qualifier } =  $self->transform_cell($cell, 1);

    }

    return $map;
}

sub transform_cell {

    my ($self, $cell, $flatten) = @_;

    warn "Unexpected cell type @{[$cell->get_cell_type]}" unless $cell->get_cell_type == 4;

    return $cell->get_value if $flatten;

    return {
            value     => $cell->get_value,
            timestamp => $cell->get_timestamp,
        };

}

package
    HBase::SimpleScanner;

use v5.14;
use warnings;

use parent 'HBase::Scanner';

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