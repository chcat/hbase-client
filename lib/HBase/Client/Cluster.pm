package HBase::Client::Cluster;

use v5.14;
use warnings;

use HBase::Client::TableScanner;
use HBase::Client::Proto::Loader;
use HBase::Client::Utils;
use HBase::Client::Try;
use HBase::Client::Region;
use HBase::Client::Table;
use HBase::Client::MetaTable;

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

    $self->{tables}->{+meta_table_name} = HBase::Client::MetaTable->new( cluster => $self );

    return $self;

}

sub meta_server {

    my ($self) = @_;

    return $self->{meta_holder_locator}->locate;

}

sub get_node {

    my ($self, $server) = @_;

    return $self->{node_pool}->get_node( $server );

}

sub table {

    my ($self, $table_name) = @_;

    return $self->{tables}->{$table_name} //= HBase::Client::Table->new(
            cluster     => $self,
            name        => $table_name,
        );

}

sub shutdown {

    my ($self) = @_;

    return $self->{node_pool}->shutdown;

}

1;