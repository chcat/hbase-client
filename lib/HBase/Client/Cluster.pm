package HBase::Client::Cluster;

use v5.14;
use warnings;

use HBase::Client::TableScanner;
use HBase::Client::Proto::Loader;
use HBase::Client::Utils;
use HBase::Client::Try;
use HBase::Client::Region;
use HBase::Client::Table;

use Promises qw( deferred );

sub new {

    my ($class, %args) = @_;

    my $meta_holder_locator = $args{meta_holder_locator};

    my $node_pool = $args{node_pool};

    my $self = bless {
            meta_holder_locator => $meta_holder_locator,
            node_pool           => $node_pool,
            tables              => {},
        }, $class;

    return $self;

}

sub get {

    my ($self, $table, $get) = @_;

    return deferred->reject('Getting the closest row before is deprecated, use reverse scan instead!')->promise if $get->{closest_row_before};

    try {

        return $self->_get_region( $table, $get->{row} )
            ->then( sub {

                    my ($region) = @_;

                    return $region->get_async( $get );
                } )
            ->catch( sub {

                    my ($error) = @_;

                    if (exception($error) eq 'org.apache.hadoop.hbase.NotServingRegionException' ){

                        retry( count => 3, cause => "Got org.apache.hadoop.hbase.NotServingRegionException" );

                    } else {

                        die $error;

                    }

                } );

    };

}

sub mutate {

    my ($self, $table, $mutation, $condition, $nonce_group) = @_;

    try {

        return $self->_get_region( $table, $mutation->{row} )
            ->then( sub {
                    my ($region) = @_;

                    return $region->mutate_async( $mutation, $condition, $nonce_group );
                } )
            ->catch( sub {

                    my ($error) = @_;

                    if (exception($error) eq 'org.apache.hadoop.hbase.NotServingRegionException' ){

                        retry( count => 3, cause => "Got org.apache.hadoop.hbase.NotServingRegionException" );

                    } else {

                        die $error;

                    }

                } );
    };

}

sub _table {

    my ($self, $table_name) = @_;

    return $self->{tables}->{$table_name} //= HBase::Client::Table->new(
            cluster     => $self,
            name        => $table_name,
        );

}

sub scanner {

    my ($self, $table_name, $scan, $number_of_rows) = @_;

    return HBase::Client::TableScanner->new(
            table               => $self->_table( $table_name ),
            scan                => $scan,
            number_of_rows      => $number_of_rows,
        );

}

sub _get_region {

    my ($self, $table_name, $row) = @_;

    return $self->get_meta_region if $table_name eq meta_table_name;

    return $self->_table( $table_name )->region( $row );

}

sub get_node {

    my ($self, $server) = @_;

    return $self->{node_pool}->get_node( $server );

}

sub invalidate_meta_region {

    my ($self) = @_;

    return undef $self->{meta_region};

}

sub get_meta_region {

    my ($self) = @_;

    return $self->{meta_region} //= try {
        $self->{meta_holder_locator}->locate->then( sub {

                    my ($server) = @_;

                    return HBase::Client::Region->new(
                        name        => 'hbase:meta,,1',
                        server      => $server,
                        start       => '',
                        end         => '',
                        cluster     => $self,
                        table       => meta_table_name,
                    );

                }, sub {

                    my ($error) = @_;

                    retry(count => 3, cause => $error);

                } );
    }->catch( sub {

            undef $self->{meta_region}; # clear cache of the failed region promise not to leave the client broken

        } );

}

1;