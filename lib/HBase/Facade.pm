package HBase::Facade;

use v5.14;
use warnings;

sub new {

    my ($class, %args) = @_;

    return bless { client => $args{client} }, $class;

}

# $table, $row, {columns => ["$family1", "$family2:$column2"], from => $from, to => $to, max_versions => $mv, existence_only => $eo, timestamped => $ts}
# returns { "$family1:$column1" => $value1, "$family2:$column2" => $value2,...  }
sub get_async {

    my ($self, $table, $row, $params) = @_;

    my $get_proto = {
            row             => $row,
            max_versions    => $params->{max_versions} // 1,
            existence_only  => $params->{existence_only} // 0,
        };

    $get_proto->{time_range}->{from} = $params->{from} if defined $params->{from};
    $get_proto->{time_range}->{to} = $params->{to} if defined $params->{to};

    if (my $columns = $params->{columns}){

        my %columns_map;
        my $columns_proto = $get_proto->{column} = [];

        for my $column (@$columns){

            my ($family, $qualifier) = split ':', $column, 2;

            my $family_qualifiers = $columns_map{ $family };

            push @$columns_proto, { family => $family, qualifier => $columns_map{ $family } = $family_qualifiers = [] } unless $family_qualifiers;

            push @$family_qualifiers, $qualifier if $qualifier;
        }

    }

    return $self->{client}->get_async( $table, $get_proto )->then( sub {

            my ($response) = @_;

            my $cells = $response->get_result->get_cell_list;

            return undef unless $cells and @$cells;

            return $self->_transform_cell_array( $cells, $get_proto->{max_versions} > 1 || $params->{timestamped})->{$row};

        } );

}

sub get { sync { shift->get_async( @_ ) }; }

# $table, $row => { "$family1:$column1" => $value1, "$family2:$column2" => $value2,...  }, { timestamp => $ts, nonce => $n  }
sub put_async {

    my ($self, $table, $row, $value, $params) = @_;

    my %columns_map;
    my @column_value_proto;

    for my $key (keys %$value) {

        my ($family, $qualifier) = split ':', $key, 2;

        my $qualifier_values = $columns_map{ $family };

        push @column_value_proto, { family => $family, qualifier_value => $columns_map{ $family } = $qualifier_values = [] } unless $qualifier_values;

        push @$qualifier_values, { qualifier => $qualifier, value => $value->{ $key } };
    }

    my $mutation = {
            row           => $row,
            mutate_type   => HBase::Client::Proto::MutationProto::MutationType::PUT,
            column_value  => \@column_value_proto,

            $params ? %$params : (),
        };

    return $self->{client}->mutate_async($table, $mutation);
}

sub put { sync { shift->put_async( @_ ) }; }

sub scanner {

    my ($self, $table, $scan, $number_of_rows) = @_;

    return HBase::Facade::Scanner->new(
            scanner         => $self->{client}->scanner( $table, $scan, $number_of_rows ),
            multi_versions  => ($args{scan}->{max_versions} // 1) > 1,
            facade          => $self,
        );

}

sub _transform_cell_array {

    my ($self, $cells, $multi_versions, $rows_map) = @_;

    return $multi_versions ? $self->_transform_cell_array_multi_versions( $cells, $rows_map ) : $self->_transform_cell_array_single_version( $cells, $rows_map );

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

package HBase::Facade::Scanner;

use v5.14;
use warnings;

sub new {

    my ($class, %args) = @_;

    return bless {
            scanner        => $args{scanner},
            multi_versions => $args{multi_versions},
            facade         => $args{facade},
        }, $class;
}

sub next_async {

    my $self = shift;

    return $self->{scanner}->next_async( @_ )
        ->then( sub {

                my ($response) = @_;

                return undef unless $response;

                my $rows = {};

                $self->{facade}->_transform_cell_array( $_->get_cell_list // [], $self->{multi_versions}, $rows ) for @{$response->get_results_list};

                return $rows;

            } );

}

sub next { sync { shift->next( @_ ) }; }

1;