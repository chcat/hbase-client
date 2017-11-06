package HBase::Facade;

use v5.14;
use warnings;

use HBase::Client::Try qw( sync );
use Promises qw( collect );
use Ref::Util qw( is_hashref is_arrayref );

sub new {

    my ($class, %args) = @_;

    return bless { client => $args{client} }, $class;

}

sub pull_stats { shift->{client}->pull_stats( @_ ); }

sub prepare { sync shift->prepare_async( @_ ); }

sub prepare_async {

    my ($self, $options) = @_;

    return $self->{client}->prepare_async( $options );

}

sub get { sync shift->get_async( @_ ); }

# $table, $row, {columns => ["$family1", "$family2:$column2"], from => $from, to => $to, max_versions => $mv, existence_only => $eo, timestamped => $ts}
# returns { "$family1:$column1" => $value1, "$family2:$column2" => $value2,...  }
sub get_async {

    my ($self, $table, $rows, $params, $options) = @_;

    return collect( map { $self->_get_single_async( $table, $_, $params, $options ) } @$rows )
        ->then( sub {

                return [ map { $_->[0] } @_ ];

            } ) if is_arrayref( $rows );

    return $self->_get_single_async( $table, $rows, $params, $options );

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

            return $self->_transform_row( $cells, $get_proto->{max_versions} > 1, $params->{timestamped});

        } );

}

sub delete { sync shift->delete_async( @_ ); }

sub delete_async {

    my ($self, $table, $rows, $params, $options) = @_;

    return collect( map { $self->_delete_single_async( $table, $_, $params, $options ) } @$rows )
        ->then( sub {

                return [ map { $_->[0] } @_ ];

            } ) if is_arrayref( $rows );

    return $self->_delete_single_async( $table, $rows, $params, $options );

}

sub _delete_single_async {

    my ($self, $table, $row, $params, $options) = @_;

    my $mutation = {
            row           => $row->{row},
            mutate_type   => HBase::Client::Proto::MutationProto::MutationType::DELETE,
        };

    $mutation->{column_value} = $self->_parse_column_value_array_proto( $params->{columns} ) if $params->{columns};

    return $self->{client}->mutate_async($table, $mutation, undef, undef, $options);

}

sub put { sync shift->put_async( @_ ); }

# $table, { row => $row, "$family1:$column1" => $value1, "$family2:$column2" => $value2,...  }, { timestamp => $ts, nonce => $n  }
sub put_async {

    my ($self, $table, $rows, $params, $options) = @_;

    return collect( map { $self->_put_single_async( $table, $_, $params, $options ) } @$rows )
        ->then( sub {

                return [ map { $_->[0] } @_ ];

            } ) if is_arrayref( $rows );

    return $self->_put_single_async( $table, $rows, $params, $options );
}

sub _put_single_async {

    my ($self, $table, $row, $params, $options) = @_;

    my $mutation = {
            row           => $row->{row},
            mutate_type   => HBase::Client::Proto::MutationProto::MutationType::PUT,
            column_value  => $self->_parse_column_value_array_proto( $row ),
        };

    return $self->{client}->mutate_async($table, $mutation, undef, undef, $options);
}

sub scanner {

    my ($self, $table, $params, $options) = @_;

    return HBase::Facade::Scanner->_new(
            scanner         => $self->{client}->scanner( $table, $params, $options ),
            multi_versions  => ($params->{max_versions} // 1) > 1,
            timestamped     => $params->{timestamped},
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

    my ($self, $columns_value) = @_;

    my (@column_value_proto, %columns_map);

    my $columns_only = is_arrayref($columns_value);

    for my $key ( $columns_only ? @$columns_value : keys %$columns_value) {

        next if $key eq 'row';

        my ($family, $qualifier) = split ':', $key, 2;

        my $qualifier_values = $columns_map{ $family };

        push @column_value_proto, { family => $family, qualifier_value => $columns_map{ $family } = $qualifier_values = [] } unless $qualifier_values;

        push @$qualifier_values, { qualifier => $qualifier, $columns_only ? () : (value => $columns_value->{ $key }) };
    }

    return \@column_value_proto;
}

sub _transform_row {

    my ($self, $cells, $multi_versions, $timestamped) = @_;

    return undef unless $cells && @$cells;

    my $row = { row => $cells->[0]->get_row };

    if ($multi_versions){

        my %to_sort;

        for my $cell (@$cells){

            my $values = $row->{ $cell->get_family . ':' . $cell->get_qualifier  } //= [];

            push @$values, $cell;

            $to_sort{$values} = $values;

        }

        @$_ = map { $self->_transform_cell( $_, $timestamped ) } sort { $b->get_timestamp <=> $a->get_timestamp } @$_ for values %to_sort;

    } else {

        $row->{ $_->get_family . ':' . $_->get_qualifier } =  $self->_transform_cell( $_, $timestamped ) for @$cells;

    }

    return $row;

}

sub _transform_cell {

    my ($self, $cell, $timestamped) = @_;

    warn "Unexpected cell type @{[$cell->get_cell_type]}" unless $cell->get_cell_type == 4;

    return $cell->get_value unless $timestamped;

    return {
            value     => $cell->get_value,
            timestamp => $cell->get_timestamp,
        };

}

package HBase::Facade::Scanner;

use v5.14;
use warnings;

use HBase::Client::Try qw( sync );

sub next { sync shift->next_async( @_ ); }

sub next_async {

    my ($self, $options) = @_;

    return $self->{scanner}->next_async( $options )
        ->then( sub {

                my ($results) = @_;

                return $results ? [ map { $self->{facade}->_transform_row( $_->get_cell_list, $self->{multi_versions}, $self->{timestamped}) } @$results ] : undef;

            } );

}

sub _new {

    my ($class, %args) = @_;

    return bless {
            scanner        => $args{scanner},
            multi_versions => $args{multi_versions},
            facade         => $args{facade},
            timestamped    => $args{timestamped},
        }, $class;
}

1;