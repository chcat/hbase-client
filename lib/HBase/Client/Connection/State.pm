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

sub write {

    my ($self, $callback, $data_ref) = @_;

    my $write_queue = $self->connection->write_queue;

    push @$write_queue, $write;

}

sub connection {

    my ($self) = @_;

    return $self->{connection};

}

1;


