package HBase::Client::Node;

use v5.14;
use warnings;

use HBase::Client::Proto::Loader;
use HBase::Client::RPC;

use constant GET => { name => 'Get', response_type=>'HBase::Client::Proto::GetResponse' };
use constant MUTATE => { name => 'Mutate', response_type=>'HBase::Client::Proto::MutateResponse' };


sub new {

    my ($class, %args) = @_;

    my $self = bless {
            rpc     => $args{rpc},
        }, $class;

    return $self;
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
            defined $nonce_group ? (nonce_group => $nonce_group) : (),
        } );

    return $self->_rpc_call_async( MUTATE, $request );

}

sub _rpc_call_async { shift->{rpc}->call_async( @_ ); }

1;