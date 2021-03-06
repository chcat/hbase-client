package HBase::Client::Node;

use v5.14;
use warnings;

use Scalar::Util qw( weaken );

sub new {

    my ($class, %args) = @_;

    my $self = bless {
            rpc                     => $args{rpc},
            pool                    => $args{pool},
            server                  => $args{server},
            pending_requests_count  => 0,
            connected               => undef,
        }, $class;

    weaken $self->{pool};

    return $self;
}

sub disconnect {

    my ($self, $reason) = @_;

    $self->_rpc->disconnect( $reason ) if $self->{connected};

    return;

}

sub query {

    my ($self, $query, $options) = @_;

    # prevents the pool from disconnecting the node while we have pending calls
    $self->_pool->block_disconnecting( $self ) if $self->{pending_requests_count}++ == 0;

    return $self->_connected->then( sub {

            my ($connected_rpc) = @_;

            $options->{stats} //= {};

            return $connected_rpc->make_call( $query->to_rpc_call, $options )->finally( sub {

                    # allows the pool to disconnect the node if there are no pending calls
                    $self->_pool->unblock_disconnecting( $self ) if --$self->{pending_requests_count} == 0; # TODO: handle scans properly

                } );

        } );

}

sub _connected {

    my ($self) = @_;

    return $self->{connected} //= $self->_connect;

}

sub _connect {

    my ($self) = @_;

    return $self->_reserve_connection
        ->then( sub {

                $self->_rpc->connect;

            } )
        ->then( sub {

                my ($connected_rpc) = @_;

                $connected_rpc->disconnected->then( sub {

                        my ($reason) = @_;

                        undef $self->{connected};

                        $self->_release_connection;

                        return;

                    } );

                return $connected_rpc;

            }, sub {

                my ($error) = @_;

                undef $self->{connected};

                $self->_release_connection;

                die $error;

            } );

}

sub _reserve_connection { $_[0]->_pool->reserve_connection( $_[0] ) }

sub _release_connection { $_[0]->_pool->release_connection( $_[0] ) }

sub _pool { $_[0]->{pool} }

sub _rpc { $_[0]->{rpc} }

1;
