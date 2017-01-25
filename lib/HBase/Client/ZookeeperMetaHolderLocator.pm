package HBase::Client::ZookeeperMetaHolderLocator;

use v5.14;
use warnings;

use HBase::Client::Proto::Loader;
use Net::ZooKeeper;
use Promises qw( deferred );

sub new {

    my ($class, %args) = @_;

    return bless {
            zookeeper_quorum => $args{zookeeper_quorum},
            zookeeper_path   => $args{zookeeper_path} // '/hbase/meta-region-server', #/
        }, $class;

}

sub locate {

    my ($self) = @_;

    my $node = Net::ZooKeeper->new( $self->{zookeeper_quorum} )->get( $self->{zookeeper_path} );

    my ( $magic, $id_length, $blob ) = unpack ( 'CNA*', $node );

    die "Unexpected zookeeper $self->{zookeeper_quorum} hbase node $self->{zookeeper_path} content" unless $magic == 255;

    my $server = HBase::Client::Proto::MetaRegionServer->decode( substr $blob, length('PBUF') + $id_length );

    warn "Unexpected RPC version $server->get_rpc_version" unless $server->get_rpc_version == 0;

    die "Meta holder is not ready" unless $server->get_state() == 3;

    my $server = $server->get_server;

    my $deferred = deferred;

    $deferred->resolve( $server->get_host_name .':'.$server->get_port );

    return $deferred->promise;
}

1;