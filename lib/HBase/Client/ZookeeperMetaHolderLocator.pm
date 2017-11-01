package HBase::Client::ZookeeperMetaHolderLocator;

# TODO: find a more async-friendly zookeeper client

use v5.14;
use warnings;

use HBase::Client::Proto::Loader;
use Net::ZooKeeper;
use Promises qw( deferred );

sub new {

    my ($class, %args) = @_;

    return bless {
            quorum => $args{quorum},
            path   => $args{path} // '/hbase/meta-region-server', #/
        }, $class;

}

sub locate {

    my ($self) = @_;

    my $deferred = deferred;

    eval {

        my $node = Net::ZooKeeper->new( $self->{quorum} )->get( $self->{path} );

        die "zookeeper unreachable or node does not exists" unless $node;

        my ( $magic, $id_length, $blob ) = unpack ( 'CNA*', $node );

        die "unexpected zookeeper node content" unless $magic == 255;

        my $server_info = HBase::Client::Proto::MetaRegionServer->decode( substr $blob, length('PBUF') + $id_length );

        warn "Unexpected RPC version @{[$server_info->get_rpc_version]}" unless $server_info->get_rpc_version == 0;

        die "meta holder is not ready" unless $server_info->get_state() == 3;

        my $server = $server_info->get_server;

        $deferred->resolve( $server->get_host_name .':'.$server->get_port );

        1;
    } or do { $deferred->reject("Error locating meta holder via zookeeper @{[$self->{quorum}]} node @{[$self->{path}]}: $@") };

    return $deferred->promise;
}

1;