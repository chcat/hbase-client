package HBase::Client::Region;

use v5.14;
use warnings;

use HBase::Client::Query::ExecService;
use HBase::Client::Query::Scan;
use HBase::Client::Query::Get;
use HBase::Client::Query::Mutate;

use HBase::Client::RegionScanner;

use HBase::Client::Utils qw(
        region_specifier
        cell_array_to_row_map
        getter
    );
use HBase::Client::Context qw( context );
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

    my $region_info_encoded = $description->{info}{regioninfo}[0]{value} // return undef;
    my $region_info = HBase::Client::Proto::RegionInfo->decode( substr $region_info_encoded, 4 );

    my $start = $region_info->get_start_key // return undef;
    my $end = $region_info->get_end_key // return undef;

    my $table_namespace = $region_info->get_table_name->get_namespace;
    my $table_qualifier = $region_info->get_table_name->get_qualifier // return undef;
    my $table_name = $table_namespace eq 'default' ? $table_qualifier : $table_namespace.':'.$table_qualifier;

    return $class->new(
            name        => $name,
            table_name  => $table_name,
            server      => $description->{info}{server}[0]{value},
            start       => $start,
            end         => $end,
            is_offline  => $region_info->get_offline,
            is_split    => $region_info->get_split,
            cluster     => $cluster,
        );
}

sub get_async {
    my ($self, $get) = @_;

    my $query = HBase::Client::Query::Get->new(
            region => $self->_specifier,
            get    => $get,
        );

    return $self->_query( $query );

}

sub mutate_async {
    my ($self, $mutation, $condition, $nonce_group) = @_;

    my $query = HBase::Client::Query::Mutate->new(
            region      => $self->_specifier,
            mutation    => $mutation,
            condition   => $condition,
            nonce_group => $nonce_group,
        );

    return $self->_query( $query );

}

sub scan_async {
    my ($self, $scan, $scanner_id, $number_of_rows, $next_call_seq, $close_scanner) = @_;

    my $query = HBase::Client::Query::Scan->new(
            region         => $self->_specifier,
            scan           => $scan,
            scanner_id     => $scanner_id,
            number_of_rows => $number_of_rows,
            next_call_seq  => $next_call_seq,
            close_scanner  => $close_scanner,
        );

    return $self->_query( $query );

}

sub exec_service_async {
    my ($self, $call) = @_;

    my $query = HBase::Client::Query::ExecService->new(
            region => $self->_specifier,
            call    => $call,
        );

    return $self->_query( $query );
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

    *{$_} = getter( $_ ) for qw( name start end server table_name cluster is_offline is_split );

}

sub _specifier { region_specifier( $_[0]->name ) }

sub _query {
    my ($self, $query) = @_;

    context->region_query_start( $self, $query );

    return $self->cluster->get_node( $self->server )->query( $query )
        ->then( sub {

                context->region_query_success;

                return @_;

            }, sub {

                context->region_query_failure;

                die @_;

            } );
}

1;
