package HBase::Client::Connection::State;

use v5.14;
use warnings;

use Scalar::Util qw( weaken );

sub new {

    my ($class, %args) = @_;

    my $self = bless {
            connection  => $args{connection},
        }, $class;

    weaken $self->{connection};

    return $self;

}

sub connection {

    my ($self) = @_;

    return $self->{connection};

}

1;


