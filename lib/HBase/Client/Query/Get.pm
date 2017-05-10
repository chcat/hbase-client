package HBase::Client::Query::Get;

use v5.14;
use warnings;

use parent 'HBase::Client::Query::Abstract';

sub to_rpc_call {

    my ($self) = @_;

    return {
            method        => 'Get',
            response_type => 'HBase::Client::Proto::GetResponse',
            param         => HBase::Client::Proto::GetRequest->new( {
                    region => $self->{region},
                    get    => $self->{get},
                } ),
        };
}

1;