package HBase::SimpleClient;

use v5.14;
use warnings;

use parent 'HBase::Client';

use HBase::SimpleScanner;

sub new { shift->SUPER::new( @_ ); }

sub get_async {

    my ($self, $table, $get) = @_;

    $self->SUPER::get_async( $table, $get )->then( sub {

            my ($response) = @_;

            return $self->_transform_cell_array( $response->get_result->get_cell_list, $get->{max_versions} // 1 > 1);

        } );

}

sub mutate_async {

    shift->SUPER::mutate_async( @_ );

}

sub scan {

    my ($self, $table, $scan, $number_of_rows) = @_;

    return HBase::SimpleScanner->new(
            client          => $self,
            table           => $table,
            scan            => $scan,
            number_of_rows  => $number_of_rows,
        );

}

sub _transform_cell_array {

    my ($self, $cells, $multi_versions, $map) = @_;

    return $multi_versions ? $self->_transform_cell_array_multi_versions( $cells, $map ) : $self->_transform_cell_array_single_version( $cells, $map );

}

sub _transform_cell_array_multi_versions {

    my ($self, $cells, $map) = @_;

    my %to_sort;

    for my $cell (@$cells){

        my $values = $map->{ $cell->get_row }->{ $cell->get_family . ':' . $cell->get_qualifier  } //= [];

        push @$values, $cell;

        $to_sort{$values} = $values;

    }

    @$_ = map { $self->_transform_cell( $_ ) } sort { $b->get_timestamp <=> $a->get_timestamp } @$_ for values %to_sort;

    return $map;

}

sub _transform_cell_array_single_version {

    my ($self, $cells, $map) = @_;

    for my $cell (@$cells){

        $map->{ $cell->get_row }->{ $cell->get_family . ':' . $cell->get_qualifier } =  $self->_transform_cell($cell, 1);

    }

    return $map;
}

sub _transform_cell {

    my ($self, $cell, $flatten) = @_;

    warn "Unexpected cell type @{[$cell->get_cell_type]}" unless $cell->get_cell_type == 4;

    return $cell->get_value if $flatten;

    return {
            value     => $cell->get_value,
            timestamp => $cell->get_timestamp,
        };

}

1;