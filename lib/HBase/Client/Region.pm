package HBase::Client::Region;

use v5.14;
use warnings;

use HBase::Client::Query::ExecService;
use HBase::Client::Query::Scan;
use HBase::Client::Query::Get;
use HBase::Client::Query::Mutate;

use HBase::Client::RegionScanner;

use List::Util qw( any );

use HBase::Client::Error qw(
        connection_error
        timeout_error
        exception_error
        region_error
        is_connection_error
        is_timeout_error
        is_exception_error
    );

use HBase::Client::Utils qw(
        region_specifier
        cell_array_to_row_map
        getter
    );
use Scalar::Util qw( weaken );

use Promises qw( deferred );


sub new {
    my ($class, %args) = @_;

    my $self = bless {
            name        => $args{name},
            table_name  => $args{table_name},
            server      => $args{server},
            start       => $args{start},
            end         => $args{end},
            is_invalid  => $args{is_invalid},
            cluster     => $args{cluster},
        }, $class;

    weaken $self->{cluster};

    return $self;

}

sub dummy {

    my ($class, %args) = @_;

    state $dummy_counter = 0;

    my $self = bless {
            name        => 'DUMMY'.$dummy_counter++,
            table_name  => $args{table_name},
            start       => $args{start},
            end         => $args{end},
            is_invalid  => region_error( 'Region is dummy' ),
        }, $class;

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

    my $is_invalid;

    $is_invalid = region_error( 'Region is offline' ) if $region_info->get_offline;
    $is_invalid = region_error( 'Region is split' ) if $region_info->get_split;

    return $class->new(
            name        => $name,
            table_name  => $table_name,
            server      => $description->{info}{server}[0]{value},
            start       => $start,
            end         => $end,
            is_invalid  => $is_invalid,
            cluster     => $cluster,
        );
}

sub get_async {
    my ($self, $get) = @_;

    my $query = HBase::Client::Query::Get->new(
            region => $self->_specifier,
            get    => $get,
        );

    return $self->_query( $query, 1 );

}

sub mutate_async {
    my ($self, $mutation, $condition, $nonce_group) = @_;

    my $query = HBase::Client::Query::Mutate->new(
            region      => $self->_specifier,
            mutation    => $mutation,
            condition   => $condition,
            nonce_group => $nonce_group,
        );

    return $self->_query( $query, 1 );

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

    return $self->_query( $query, 1 );

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

    *{$_} = getter( $_ ) for qw( name start end server table_name cluster is_invalid);

}

sub _specifier { region_specifier( $_[0]->name ) }

sub _query {
    my ($self, $query, $invalidate_on_timeout) = @_;

    # If the region is already known to be invalid, throw a retryable exception after invalidation completes.
    # There are a few scenarios when we query an invalid region (like a dummy one, or a just invalid one that
    # was not invalidated successfully) and we would like the caller to retry AFTER the current invalidation
    # attempt
    return $self->invalidate->then( sub { die $self->is_invalid; }, sub { die $self->is_invalid; } ) if ($self->is_invalid);

    # These signalize we better try somewhere else
    state $invalidating_exceptions = [ qw (
            org.apache.hadoop.hbase.exceptions.RegionOpeningException
            org.apache.hadoop.hbase.exceptions.RegionInRecoveryException
            org.apache.hadoop.hbase.exceptions.RegionMovedException
            org.apache.hadoop.hbase.NotServingRegionException
            org.apache.hadoop.hbase.regionserver.RegionServerAbortedException
            org.apache.hadoop.hbase.regionserver.RegionServerStoppedException
        )];

    return $self->cluster->get_node( $self->server )->query( $query )->catch( sub {

            my ($error) = @_;

            if (is_connection_error( $error ) || ( is_timeout_error( $error ) && $invalidate_on_timeout ) ||
                is_exception_error( $error ) && any { $error->exception_class eq $_ } @$invalidating_exceptions ){

                # mark the region as invalid
                $self->{is_invalid} = region_error( $error );

                # throw a retryable exception after invalidation completes
                return $self->invalidate->then( sub { die $self->is_invalid; }, sub { die $self->is_invalid; } );

            }

            # an error like coprocessor one
            die $error;

        } );
}

sub invalidate {

    my ($self) = @_;

    return $self->{invalidating} //= $self->cluster->table( $self->table_name )
        ->invalidate( $self )
        ->finally( sub {

                # upon successfull invalidation the invalid region is replaced with a new on (possible invalid as well)
                # if it did not happen, we still will be getting

                undef $self->{invalidating};

            } );


}

1;
