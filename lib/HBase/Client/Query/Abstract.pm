package HBase::Client::Query::Abstract;

use v5.14;
use warnings;

use HBase::Client::Proto::Loader;

sub new {

    my ($class, %args) = @_;

    return bless { %args }, $class;

}

sub type {

    my ($self) = @_;

    return $self->{type};

}

1;