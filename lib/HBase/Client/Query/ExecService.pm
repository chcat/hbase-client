package HBase::Client::Query::ExecService;

use v5.14;
use warnings;

use parent 'HBase::Client::Query::Abstract';

sub to_rpc_call {

    my ($self) = @_;

    my $request = HBase::Client::Proto::CoprocessorServiceRequest->new( {
            region => $self->{region},
            call   => $self->{call},
        } );

    return {
            method        => 'ExecService',
            response_type => 'HBase::Client::Proto::CoprocessorServiceResponse',
            param         => $request
        };
}

1;