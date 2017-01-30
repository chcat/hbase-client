package HBase::Client::NodePool;

use v5.14;
use warnings;

use HBase::Client::Connection::Opened;
use HBase::Client::Node;
use HBase::Client::RPC;

use Promises qw( deferred );

sub new {

    my ($class, %args) = @_;

    my $self = bless {
            nodes => {},
        }, $class;

}


sub get_node {

    my ($self, $server) = @_;

    return $self->{nodes}->{$server} //= $self->_discover_node( $server );

}

sub _discover_node {

    my ($self, $server) = @_;

    my ($host, $port) = split ':', $server;

    my $connection = HBase::Client::Connection::Opened->new(
            host                => $host,
            port                => $port,
            connect_timeout     => 30,
            write_timeout       => 30,
        );

    my $rpc = HBase::Client::RPC->new( connection => $connection, timeout => 30 );

    my $node = HBase::Client::Node->new( rpc => $rpc );

    my $deferred = deferred;

    $node->connect( sub {

            my ($error) = @_;

            if (defined $error){

                $deferred->reject( $error );

            } else {

                $deferred->resolve( $node );

            }

        } );

    return $deferred->promise;

}

1;