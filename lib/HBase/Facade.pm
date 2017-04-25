package HBase::Facade;

use v5.14;
use warnings;

use HBase::Client::Try qw( sync );
use Promises qw( collect );

sub new {

    my ($class, %args) = @_;

    return bless { client => $args{client} }, $class;

}

# $table, $row, {columns => ["$family1", "$family2:$column2"], from => $from, to => $to, max_versions => $mv, existence_only => $eo, timestamped => $ts}
# returns { "$family1:$column1" => $value1, "$family2:$column2" => $value2,...  }
sub get_async {

    my ($self, $table, $rows, $params, $options) = @_;

    return $self->_get_single_async( $table, $rows, $params, $options ) if ref($rows) eq '';

    return collect( map { $self->_get_single_async( $table, $_, $params, $options ) } @$rows )
        ->then( sub {

                return [ map { $_->[0] } @_ ];

            } );

}

sub _get_single_async {

    my ($self, $table, $row, $params, $options) = @_;

    my $get_proto = {
            row             => $row,
            max_versions    => $params->{max_versions} // 1,
            existence_only  => $params->{existence_only} // 0,
        };

    $get_proto->{time_range}->{from} = $params->{from} if defined $params->{from};
    $get_proto->{time_range}->{to} = $params->{to} if defined $params->{to};

    $get_proto->{column} = $self->_parse_column_array_proto( $params->{columns} ) if $params->{columns};

    return $self->{client}->get_async( $table, $get_proto, $options )->then( sub {

            my ($response) = @_;

            my $cells = $response->get_result->get_cell_list;

            return undef unless $cells and @$cells;

            return $self->_transform_cell_array( $cells, $get_proto->{max_versions} > 1 || $params->{timestamped})->{$row};

        } );

}

sub get { sync shift->get_async( @_ ); }

# $table, $row => { "$family1:$column1" => $value1, "$family2:$column2" => $value2,...  }, { timestamp => $ts, nonce => $n  }
sub put_async {

    my ($self, $table, $row, $value, $params, $options) = @_;

    my $mutation = {
            row           => $row,
            mutate_type   => HBase::Client::Proto::MutationProto::MutationType::PUT,
            column_value  => $self->_parse_column_value_array_proto( $value ),

            $params ? %$params : (),
        };

    return $self->{client}->mutate_async($table, $mutation, undef, undef, $options);
}

sub put { sync shift->put_async( @_ ); }

sub scanner {

    my ($self, $table, $params, $options) = @_;

    return HBase::Facade::Scanner->_new(
            scanner         => $self->{client}->scanner( $table, $params, $options ),
            multi_versions  => ($params->{max_versions} // 1) > 1 || $params->{timestamped},
            facade          => $self,
        );

}

sub _parse_column_array_proto {

    my ($self, $columns) = @_;

    my @columns_proto;
    my %columns_map;

    for my $column (@$columns){

        my ($family, $qualifier) = split ':', $column, 2;

        my $family_qualifiers = $columns_map{ $family };

        push @columns_proto, { family => $family, qualifier => $columns_map{ $family } = $family_qualifiers = [] } unless $family_qualifiers;

        push @$family_qualifiers, $qualifier if $qualifier;
    }

    return \@columns_proto;

}

sub _parse_column_value_array_proto {

    my ($self, $row_value) = @_;

    my @column_value_proto;
    my %columns_map;

    for my $key (keys %$row_value) {

        my ($family, $qualifier) = split ':', $key, 2;

        my $qualifier_values = $columns_map{ $family };

        push @column_value_proto, { family => $family, qualifier_value => $columns_map{ $family } = $qualifier_values = [] } unless $qualifier_values;

        push @$qualifier_values, { qualifier => $qualifier, value => $row_value->{ $key } };
    }

    return \@column_value_proto;
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

use HBase::Client::Try qw( sync );

sub next_async {

    my ($self, $options) = @_;

    return $self->{scanner}->next_async( $options )
        ->then( sub {

                my ($results) = @_;

                return undef unless $results;

                my $rows = {};

                $self->{facade}->_transform_cell_array( $_->get_cell_list // [], $self->{multi_versions}, $rows ) for @$results;

                return $rows;

            } );

}

sub next { sync shift->next_async( @_ ); }

sub _new {

    my ($class, %args) = @_;

    return bless {
            scanner        => $args{scanner},
            multi_versions => $args{multi_versions},
            facade         => $args{facade},
        }, $class;
}

1;