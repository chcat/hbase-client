package HBase::Client;

use v5.14;
use warnings;

our $VERSION = '0.0.1';

use HBase::Client::Sync;
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
        }, $class;

}

sub get_async { shift->_cluster->table( shift )->get( @_ ); }

sub mutate_async { shift->_cluster->table( shift )->mutate( @_ ); }

SYNC_METHODS: {

    *{get} = sync( sub { shift->get_async( @_ ) } );
    *{mutate} = sync( sub { shift->mutate_async( @_ ) } );

}

sub scan {

    my ($self, $table, $scan, $number_of_rows) = @_;

    return HBase::Client::Scanner->new(
            client          => $self,
            table           => $table,
            scan            => $scan,
            number_of_rows  => $number_of_rows,
        );

}

sub _cluster { shift->{cluster}; }

sub DESTROY {
    local $@;
    return if ${^GLOBAL_PHASE} eq 'DESTRUCT';

    my ($self) = @_;

    $self->{cluster}->shutdown;

}

package HBase::Client::Scanner;

use v5.14;
use warnings;

use HBase::Client::Sync;

sub new {

    my ($class, %args) = @_;

    return bless {
            client  => $args{client},
            scanner => $args{client}->_cluster->( $args{table} )->scanner( $args{scan}, $args{number_of_rows} // 1000 ),
        }, $class;

}

sub next_async {

    return shift->{scanner}->next( @_ );

}

SYNC_METHODS: {

    *{next} = sync( sub { shift->next_async( @_ ) } );

}

1;
