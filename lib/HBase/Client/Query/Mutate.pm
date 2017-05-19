package HBase::Client::Query::Mutate;

use v5.14;
use warnings;

use parent 'HBase::Client::Query::Abstract';

sub new {

    return shift->SUPER::new( @_, type => 'mutate' );

}

sub to_rpc_call {

    my ($self) = @_;

    my $condition = $self->{condition};

    my $nonce_group = $self->{nonce_group};

    my $request = HBase::Client::Proto::MutateRequest->new( {
            region      => $self->{region},
            mutation    => $self->{mutation},
            $condition ? (condition => $condition) : (),
            $nonce_group ? (nonce_group => $nonce_group) : (),
        } );

    return {
            method        => 'Mutate',
            response_type => 'HBase::Client::Proto::MutateResponse',
            param         => $request
        };
}

1;