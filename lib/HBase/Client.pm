package HBase::Client;

use v5.14;
use warnings;

our $VERSION = '0.0.1';

use HBase::Client::Try qw( sync timeout );
use HBase::Client::Cluster;
use HBase::Client::NodePool;
use HBase::Client::ZookeeperMetaHolderLocator;
use HBase::Client::RequestExecutionContext;

use Time::HiRes qw(time);

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
            timeout => $args{timeout} // 60,
        }, $class;

}

sub prepare { sync shift->prepare_async( @_ ); }

sub prepare_async {

    my ($self, $options) = @_;

    return $self->_cluster->prepare;

}

sub pull_stats {

    my ($self) = @_;

    return delete $self->{stats};

}

sub _do_request_with_timeout {

    my ($self, $type, $sub, $timeout) = @_;

    my $request_stats = $self->{stats}->{client_requests} //= {};

    $request_stats->{$type}->{submitted}++;

    my $start = time;

    return timeout( $timeout, $sub )->then(sub {

            my $stats = $request_stats->{$type} //= {};

            $stats->{succeeded}++;

            my $latency = int ((time - $start)*1000);

            $stats->{latency_total} += $latency;

            $stats->{latency_average} = int ($stats->{latency_total} / $stats->{succeeded});

            return @_;

        }, sub {

            $request_stats->{$type}->{failed}++;

            die @_;

        });

}

sub get { sync shift->get_async( @_ ); }

sub get_async {

    my ($self, $table, $get, $options) = @_;

    return $self->_do_request_with_timeout(
            'get',
            sub {
                $self->_cluster->table( $table )->get( $get );
            },
            $options->{timeout} // $self->{timeout}
        );

}

sub mutate { sync shift->mutate_async( @_ ); }

sub mutate_async {

    my ($self, $table, $mutation, $condition, $nonce_group, $options) = @_;

    return $self->_do_request_with_timeout(
            'mutate',
            sub {
                $self->_cluster->table( $table )->mutate( $mutation, $condition, $nonce_group );
            },
            $options->{timeout} // $self->{timeout}
        );

}

sub exec_service { sync shift->exec_service_async( @_ ); }

sub exec_service_async {

    my ($self, $table, $call, $options) = @_;

    return $self->_do_request_with_timeout(
            'exec',
            sub {
                $self->_cluster->table( $table )->exec_service( $call );
            },
            $options->{timeout} // $self->{timeout}
        );
}

sub scanner {

    my ($self, $table, $scan, $options) = @_;

    return HBase::Client::Scanner->_new(
            client          => $self,
            table           => $table,
            scan            => $scan,
            number_of_rows  => $options->{number_of_rows},
            timeout         => $options->{timeout} // $self->{timeout},
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

use HBase::Client::Try qw( sync timeout try handle retry done);
use Promises qw( deferred );

sub next_async {

    my ($self, $options) = @_;

    my $buffer = $self->{buffer};

    my $number_of_rows = $options->{number_of_rows} // $self->{number_of_rows};

    return $self->{client}->_do_request_with_timeout(
            'scan',
            sub {

                try {

                    return deferred->resolve([splice @$buffer, 0, $number_of_rows]) if $number_of_rows <= @$buffer;

                    my $timeout = $options->{timeout} // $self->{timeout};

                    $self->{scanner}->next( {number_of_rows => $number_of_rows} )->then( sub {

                            my ($response) = @_;

                            if ($response){

                                push @$buffer, @$response;

                                retry( cause => 'Got more rows' );

                            } else {

                                done($buffer);

                            }

                        } );

                };

            },
            $options->{timeout} // $self->{timeout}
        );

}

sub next { sync shift->next_async( @_ ); }

sub _new {

    my ($class, %args) = @_;

    return bless {
            client         => $args{client}, # keep the link to the client to avoid gc
            scanner        => $args{client}->_cluster->table( $args{table} )->scanner( $args{scan} ),
            number_of_rows => $args{number_of_rows} // 1000,
            timeout        => $args{timeout} // 60,
            buffer         => [],
        }, $class;

}

1;
