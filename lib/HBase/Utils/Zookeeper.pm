package HBase::Utils::Zookeeper;

use strict;
use warnings;

use HBase::Client::Proto::Loader;
use Net::ZooKeeper;

sub locateMetaRegionServer {

    my $zkh = Net::ZooKeeper->new( $_[0] );

    my $node = $zkh->get('/hbase/meta-region-server');

    my ( $magic, $id_length, $blob ) = unpack ( 'CNA*', $node );

    die "Bad node" unless $magic == 255;

    my $server = HBase::Client::Proto::MetaRegionServer->decode( substr $blob, length('PBUF') + $id_length );

    warn "Not sure we support the version" unless $server->get_rpc_version() == 0;

    die "Meta Server not ready" unless $server->get_state() == 3;

    return $server->get_server();

}

1;