package HBase::Client::NodePool;

use v5.14;
use warnings;

use HBase::Client::Connection;
use HBase::Client::Node;
use HBase::Client::RPCChannel;

use Promises qw( deferred );

sub new {

    my ($class, %args) = @_;

    my $self = bless {
            nodes                => {},
            disconnectable_nodes => {},
            connections          => 0,
            connections_limit    => $args{connections_limit} // 16,
            waiting_queue        => [],
        }, $class;

}

sub shutdown {

    my ($self) = @_;

    $self->{shutdown} = 1;

    $_->disconnect('Shutdown') for values %{$self->{nodes}};

    return;

}

sub get_node {

    my ($self, $server) = @_;

    return $self->{nodes}->{$server} //= $self->_create_node( $server );

}

sub _create_node {

    my ($self, $server) = @_;

    my ($host, $port) = split ':', $server;

    my $connection = HBase::Client::Connection->new(
            host                => $host,
            port                => $port,
            connect_timeout     => 3,
            write_timeout       => 3,
        );

    my $rpc = HBase::Client::RPCChannel->new( connection => $connection, timeout => 15 );

    my $node = HBase::Client::Node->new( rpc => $rpc, pool => $self );

    return $node;

}

sub reserve_connection {

    my ($self, $node) = @_;

    my $deferred = deferred;

    return $deferred if $self->{shutdown};

    my $limit = $self->{connections_limit};

    if (!$limit || $self->{connections} < $limit ){

        $self->{connections}++;

        $deferred->resolve;

    } else {

        push @{$self->{waiting_queue}}, $deferred;

        $self->_hustle_waiting_queue;

    }

    return $deferred->promise;

}

sub block_disconnecting {

    my ($self, $node) = @_;

    delete $self->{disconnectable_nodes}->{$node};

    return;

}

sub unblock_disconnecting {

    my ($self, $node) = @_;

    $self->{disconnectable_nodes}->{$node} = $node;

    $self->_hustle_waiting_queue;

    return;

}

sub release_connection {

    my ($self, $node) = @_;

    delete $self->{disconnectable_nodes}->{$node};

    $self->{connections}--;

    return if $self->{shutdown};

    my $waiting_queue = $self->{waiting_queue};

    if (my $deferred = shift @$waiting_queue){

        $self->{connections}++;

        $deferred->resolve;

    }

    return;

}

sub _hustle_waiting_queue {

    my ($self) = @_;

    my $waiting_queue = $self->{waiting_queue};

    my $disconnectable_nodes = $self->{disconnectable_nodes};

    if (@$waiting_queue && %$disconnectable_nodes){

        my ($node) = values %$disconnectable_nodes;

        $node->disconnect;

    }

    return;

}

1;