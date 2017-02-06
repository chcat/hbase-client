package HBase::Client::Scanner;

use v5.14;
use warnings;

use HBase::Client::Sync;

sub new {

    my ($class, %args) = @_;

    my $self = bless {
            %args,
        }, $class;

    return $self;

}

sub next_async {

    return shift->{scanner}->next_async( @_ );

}

SYNC_METHODS: {

    *{next} = sync( \&next_async );

}

1;