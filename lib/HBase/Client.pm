package HBase::Client;

use v5.14;
use warnings;

our $VERSION = '0.0.1';

use AnyEvent;
use HBase::Client::Cluster;
use HBase::Client::NodePool;
use HBase::Client::ZookeeperMetaHolderLocator;

sub new {

    my ($class, %args) = @_;

    my $meta_holder_locator;

    if (my $zookeeper = $args{zookeeper}){

        $meta_holder_locator = HBase::Client::ZookeeperMetaHolderLocator->new( %$zookeeper );

    }

    my $node_pool = HBase::Client::NodePool->new();

    my $cluster = HBase::Client::Cluster->new(
            meta_holder_locator => $meta_holder_locator,
            node_pool           => $node_pool,
        );

    return bless {
            cluster => $cluster,
        }, $class;

}

sub get_async {

    shift->{cluster}->get_async( @_ );

}

sub mutate_async {

    shift->{cluster}->mutate_async( @_ );

}

sub DESTROY {
    local $@;
    return if ${^GLOBAL_PHASE} eq 'DESTRUCT';

    my $self = shift;


    #TODO shutdown
}

sub _sync {

    my ($sub) = @_;

    return sub {

            my $done = AnyEvent->condvar;

            my ($result,$error);

            $sub->( @_ )
                ->then( sub { $result = shift; }, sub { $error = shift; } )
                ->finally( sub { $done->send; } );

            $done->recv;

            die $error if defined $error;

            return $result;

        };
}

SYNC_METHODS: {

    *{get} = _sync( \&get_async );
    *{mutate} = _sync( \&mutate_async );

}

1;
