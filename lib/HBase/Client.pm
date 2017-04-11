package HBase::Client;

use v5.14;
use warnings;

our $VERSION = '0.0.1';

use HBase::Client::Try qw( sync timeout );
use HBase::Client::Cluster;
use HBase::Client::NodePool;
use HBase::Client::ZookeeperMetaHolderLocator;

sub new {

    my ($class, %args) = @_;

    my $meta_holder_locator;

    if (my $zookeeper = $args{zookeeper}){

        $meta_holder_locator = HBase::Client::ZookeeperMetaHolderLocator->new( %$zookeeper );

    }

    my $node_pool = HBase::Client::NodePool->new( $args{node_pool} ? %{$args{node_pool}} : () );

    my $cluster = HBase::Client::Cluster->new(
            meta_holder_locator => $meta_holder_locator,
            node_pool           => $node_pool,
        );

    return bless {
            cluster => $cluster,
            timeout => $args{timeout},
        }, $class;

}

sub get_async {

    my ($self, $table, $get, $options) = @_;

    my $timeout = $options && exists $options->{timeout} ? $options->{timeout} : $self->{timeout};

    return timeout $timeout, { $self->_cluster->table( $table )->get( $get ) };

}

sub mutate_async {

    my ($self, $mutation, $condition, $nonce_group, $options) = @_;

    my $timeout = $options && exists $options->{timeout} ? $options->{timeout} : $self->{timeout};

    return timeout $timeout, { $self->_cluster->table( $table )->mutate( $mutation, $condition, $nonce_group ) };

}

sub get { sync shift->get_async( @_ ); }

sub mutate { sync shift->mutate_async( @_ ); }

sub scanner {

    my ($self, $table, $scan, $number_of_rows) = @_;

    return HBase::Client::Scanner->_new(
            client          => $self,
            table           => $table,
            scan            => $scan,
            number_of_rows  => $number_of_rows,
        );

}

sub _cluster { $_[0]->{cluster}; }

sub DESTROY {
    local $@;
    return if ${^GLOBAL_PHASE} eq 'DESTRUCT';

    my ($self) = @_;

    $self->{cluster}->shutdown;

}

package HBase::Client::Scanner;

use v5.14;
use warnings;

use HBase::Client::Try qw( sync );

sub next_async {

    my ($self, $options) = @_;

    my $timeout = $options && exists $options->{timeout} ? $options->{timeout} : $self->{client}->{timeout};

    return  timeout $timeout, { $self->{scanner}->next };

}

sub next { sync shift->next_async( @_ ); }

sub _new {

    my ($class, %args) = @_;

    return bless {
            client  => $args{client},
            scanner => $args{client}->_cluster->table( $args{table} )->scanner( $args{scan}, $args{number_of_rows} // 1000 ),
        }, $class;

}

1;
