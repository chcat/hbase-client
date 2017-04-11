package HBase::Client::Node;

use v5.14;
use warnings;

use HBase::Client::Proto::Loader;
use HBase::Client::RegionScanner;

use Scalar::Util qw( weaken );

use constant GET => { name => 'Get', response_type=>'HBase::Client::Proto::GetResponse' };
use constant MUTATE => { name => 'Mutate', response_type=>'HBase::Client::Proto::MutateResponse' };
use constant SCAN => { name => 'Scan', response_type=>'HBase::Client::Proto::ScanResponse' };

sub new {

    my ($class, %args) = @_;

    my $self = bless {
            rpc                     => $args{rpc},
            pool                    => $args{pool},
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

sub get_async {

    my ($self, $region, $get) = @_;

    my $request = HBase::Client::Proto::GetRequest->new( {
            region => $region,
            get    => $get,
        } );

    return $self->_rpc_call_async( GET, $request );

}

sub mutate_async {

    my ($self, $region, $mutation, $condition, $nonce_group) = @_;


    my $request = HBase::Client::Proto::MutateRequest->new( {
            region      => $region,
            mutation    => $mutation,
            $condition ? (condition => $condition) : (),
            $nonce_group ? (nonce_group => $nonce_group) : (),
        } );

    return $self->_rpc_call_async( MUTATE, $request );

}

sub scan_async {

    my ($self, $region, $scan, $scanner_id, $number_of_rows, $next_call_seq, $close_scanner) = @_;

    my $request = HBase::Client::Proto::ScanRequest->new( {
            defined $scanner_id ? () : (region => $region),
            defined $scanner_id ? () : (scan => $scan),
            defined $scanner_id ? (scanner_id => $scanner_id) : (),
            defined $number_of_rows ? (number_of_rows => $number_of_rows) : (),
            defined $close_scanner ? (close_scanner => $close_scanner) : (),
            defined $next_call_seq ? (next_call_seq => $next_call_seq) : (),
        } );

    return $self->_rpc_call_async( SCAN, $request );

}

sub _rpc_call_async {

    my ($self, @args) = @_;

    # prevents the pool from disconnecting the node while we have pending calls
    $self->_pool->block_disconnecting( $self ) if $self->{pending_requests_count}++ == 0;

    return $self->_connected->then( sub {

            my ($connected_rpc) = @_;

            return $connected_rpc->call_async( @args )->finally( sub {

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