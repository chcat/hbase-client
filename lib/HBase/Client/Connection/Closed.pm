package HBase::Client::Connection::Closed;

use v5.14;
use warnings;

use parent 'HBase::Client::Connection::State';

sub _enter {

    my ($self) = @_;

    my $queue = $self->{write_queue};

    for my $write ( @$queue ){



    }


}


1;