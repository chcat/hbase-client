package HBase::Client;

use v5.14;
use warnings;

our $VERSION = '0.0.1';

use HBase::Client::Proto::Loader;

sub new {

}

sub get {



}

sub DESTROY {
    local $@;
    return if ${^GLOBAL_PHASE} eq 'DESTRUCT';

    my $self= shift;
    if ($self->{connected}) {
        $self->shutdown;
    }
}

1;
