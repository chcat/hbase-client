package HBase::Client;

use v5.14;
use warnings;

our $VERSION = '0.0.1';

use HBase::Client::Try qw( sync timeout );
use HBase::Client::Cluster;
use HBase::Client::NodePool;
use HBase::Client::ZookeeperMetaHolderLocator;

use HBase::Client::Context qw( set_context );
use HBase::Client::RequestExecutionContext;


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

sub prepare {
    my ($self, $options) = @_;

    set_context( HBase::Client::RequestExecutionContext->new( $options->{stats} ) );

    return sync $self->_cluster->prepare;
}

sub get { sync shift->get_async( @_ ); }

sub get_async {

    my ($self, $table, $get, $options) = @_;

    set_context( HBase::Client::RequestExecutionContext->new( $options->{stats} ) );

    my $timeout = $options->{timeout} // $self->{timeout};

    return timeout $timeout, sub { $self->_cluster->table( $table )->get( $get ) };

}

sub mutate { sync shift->mutate_async( @_ ); }

sub mutate_async {

    my ($self, $table, $mutation, $condition, $nonce_group, $options) = @_;

    set_context( HBase::Client::RequestExecutionContext->new( $options->{stats} ) );

    my $timeout = $options->{timeout} // $self->{timeout};

    return timeout $timeout, sub { $self->_cluster->table( $table )->mutate( $mutation, $condition, $nonce_group ) };

}

sub exec_service { sync shift->exec_service_async( @_ ); }

sub exec_service_async {

    my ($self, $table, $call, $options) = @_;

    set_context( HBase::Client::RequestExecutionContext->new( $options->{stats} ) );

    my $timeout = $options->{timeout} // $self->{timeout};

    return timeout $timeout, sub { $self->_cluster->table( $table )->exec_service( $call ) };
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
use HBase::Client::Context qw( set_context );

sub next_async {

    my ($self, $options) = @_;

    set_context( HBase::Client::RequestExecutionContext->new( $options->{stats} ) );

    my $buffer = $self->{buffer};

    my $number_of_rows = $options->{number_of_rows} // $self->{number_of_rows};

    return try {

            return deferred->resolve([splice @$buffer, 0, $number_of_rows]) if $number_of_rows <= @$buffer;

            my $timeout = $options->{timeout} // $self->{timeout};

            timeout( $timeout, sub { $self->{scanner}->next( {number_of_rows => $number_of_rows} ) } )->then( sub {

                    my ($response) = @_;

                    if ($response){

                        push @$buffer, @$response;

                        retry( cause => 'Got more rows' );

                    } else {

                        done($buffer);

                    }

                } );

        }

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
