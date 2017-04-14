package HBase::Client::Cluster;

use v5.14;
use warnings;

use HBase::Client::Table;
use HBase::Client::MetaTable;

sub new {

    my ($class, %args) = @_;

    my $meta_holder_locator = $args{meta_holder_locator};

    my $node_pool = $args{node_pool};

    my $self = bless {
            meta_holder_locator => $meta_holder_locator,
            node_pool           => $node_pool,
            tables              => {},
        }, $class;

    my $meta_table = HBase::Client::MetaTable->new( cluster => $self );

    $self->{tables}->{ $meta_table->name } = $meta_table;

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
