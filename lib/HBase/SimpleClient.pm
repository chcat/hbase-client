package HBase::SimpleClient;

use v5.14;
use warnings;

use parent 'HBase::Client';

use HBase::Client::Sync;

sub new { shift->SUPER::new( @_ ); }

# $table, $row, {columns => ["$family1", "$family2:$column2"], from => $from, to => $to, max_versions => $mv, existence_only => $eo}
sub get_async {

    my ($self, $table, $row, $get) = @_;

    my $get_proto = {
            row             => $row,
            max_versions    => $get->{max_versions} // 1,
            existence_only  => $get->{existence_only} // 0,
        };

    $get_proto->{time_range}->{from} = $get->{from} if defined $get->{from};
    $get_proto->{time_range}->{to} = $get->{to} if defined $get->{to};

    if (my $columns = $get->{columns}){

        my %columns_map;
        my $columns_proto = $get_proto->{column} = [];

        for my $column (@$columns){

            my ($family, $qualifier) = split ':', $column, 2;

            my $family_qualifiers = $columns_map{ $family };

            push @$columns_proto, { family => $family, qualifier => $columns_map{ $family } = $family_qualifiers = [] } unless $family_qualifiers;

            push @$family_qualifiers, $qualifier if $qualifier;
        }

    }

    return $self->SUPER::get_async( $table, $get_proto )->then( sub {

            my ($response) = @_;

            my $cells = $response->get_result->get_cell_list;

            return undef unless $cells and @$cells;

            return $self->_transform_cell_array( $cells, $get_proto->{max_versions} > 1)->{$row};

        } );

}

sub mutate_async {

    my ($self, $table, $mutation, $condition, $nonce_group) = @_;

    return $self->SUPER::mutate_async( $table, $mutation, $condition, $nonce_group );

}

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

    return $self->mutate_async()
}

sub delete {


}

SYNC_METHODS: {

    *{put} = sync( sub { shift->put_async( @_ ) } );
    *{delete} = sync( sub { shift->delete_async( @_ )  } );

}

sub scan {

    my ($self, $table, $scan, $number_of_rows) = @_;

    return HBase::SimpleClient::Scanner->new(
            client          => $self,
            table           => $table,
            scan            => $scan,
            number_of_rows  => $number_of_rows,
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

package HBase::SimpleClient::Scanner;

use v5.14;
use warnings;

use parent -norequire, 'HBase::Client::Scanner';

sub new {

    my ($class, %args) = @_;

    my $self = $class->SUPER::new( %args );

    $self->{multi_versions} = ($args{scan}->{max_versions} // 1) > 1;

    return $self;
}

sub next_async {

    my $self = shift;

    return $self->SUPER::next_async( @_ )
        ->then( sub {

                my ($response) = @_;

                return undef unless $response;

                my $rows = {};

                $self->{client}->_transform_cell_array( $_->get_cell_list // [], $self->{multi_versions}, $rows ) for @{$response->get_results_list};

                return $rows;

            } );

}

1;