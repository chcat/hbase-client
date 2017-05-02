package HBase::Client::Region;

use v5.14;
use warnings;

use HBase::Client::Utils qw(
        region_specifier
        cell_array_to_row_map
        getter
    );
use Scalar::Util qw( weaken );


sub new {
    my ($class, %args) = @_;

    my $self = bless {
            name        => $args{name},
            table_name  => $args{table_name},
            server      => $args{server},
            start       => $args{start},
            end         => $args{end},
            is_offline  => $args{is_offline},
            cluster     => $args{cluster},
        }, $class;

    weaken $self->{cluster};

    return $self;

}

sub parse {
    my ($class, $cluster, $result) = @_;

    return undef unless $result;

    my ($name, $description) = %{ cell_array_to_row_map( $result->get_cell_list ) };

    return undef unless $name && $description;

    my $region_info_encoded = $description->{info}{regioninfo}[0]{value} // die 'Can not extract region info from result';
    my $region_info = HBase::Client::Proto::RegionInfo->decode( substr $region_info_encoded, 4 );

    my $table_namespace = $region_info->get_table_name->get_namespace;
    my $table_qualifier = $region_info->get_table_name->get_qualifier;
    my $table_name = $table_namespace eq 'default' ? $table_qualifier : $table_namespace.':'.$table_qualifier;

    return $class->new(
            name        => $name,
            table_name  => $table_name,
            server      => $description->{info}{server}[0]{value},
            start       => $region_info->get_start_key,
            end         => $region_info->get_end_key,
            is_offline  => $region_info->get_offline,
            cluster     => $cluster,
        );
}

sub get_async {
    my ($self, @args) = @_;

    return $self->_get_node->get_async( $self->_specifier, @args );

}

sub mutate_async {
    my ($self, @args) = @_;

    return $self->_get_node->mutate_async( $self->_specifier, @args );

}

sub scan_async {
    my ($self, @args) = @_;

    return $self->_get_node->scan_async( $self->_specifier, @args );

}

sub exec_service_async {
    my ($self, @args) = @_;

    return $self->_get_node->exec_service_async( $self->_specifier, @args );
}

sub scanner {

    my ($self, $scan, $number_of_rows) = @_;

    return HBase::Client::RegionScanner->new(
            region              => $self,
            scan                => $scan,
            number_of_rows      => $number_of_rows,
        );

}

GETTERS: {

    no strict 'refs';

    *{$_} = getter( $_ ) for qw( name start end server table_name cluster is_offline );

}

sub _specifier { region_specifier( $_[0]->name ) }

sub _get_node {
    my ($self) = @_;

    return $self->cluster->get_node( $self->server );
}

1;
