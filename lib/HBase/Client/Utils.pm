package HBase::Client::Utils;

use v5.14;
use warnings;

use HBase::Client::Proto::Loader;
use Exporter 'import';

our @EXPORT= qw(
        meta_table_name
        meta_region_specifier
        region_specifier
        region_name
        region_name_new_format
        cell_array_to_row_map
    );

sub meta_table_name {

    return 'hbase:meta';

}

sub meta_region_specifier {

    return region_specifier( 'hbase:meta,,1' );

}

sub region_specifier {

    my ($region_name) = @_;

    return HBase::Client::Proto::RegionSpecifier->new( {
            type    => HBase::Client::Proto::RegionSpecifier::RegionSpecifierType::REGION_NAME,
            value   => $region_name,
        } );

}

sub region_name_new_format {

    my $name = region_name( @_ );

    return $name . '.' . md5_hex( $name ) . '.';

}

sub region_name {

    my ($table, $start, $id, $replica_id) = @_;

    my $name = $table . ',' . ($start // '') . ',' . ($id // '') ;

    $name .= '_' . sprintf( '%04X', $replica_id) if $replica_id;

    return $name;

}

sub cell_array_to_row_map {

    my ($cells) = @_;

    my $map;

    my %to_sort;

    for my $cell (@$cells){

        my $values = $map->{ $cell->get_row }->{ $cell->get_family }->{ $cell->get_qualifier } //= [];

        push @$values, $cell;

        $to_sort{$values} = \$values;

    }

    $$_ = [ sort { $b->get_timestamp <=> $a->get_timestamp } @{$$_} ] for (values %to_sort);

    return $map;

}

1;