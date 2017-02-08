package HBase::Scanner;

use v5.14;
use warnings;

use HBase::Client::Sync;

sub new { return bless {@_[1..$#_]}, $_[0]; }

sub next_async {

    return shift->{scanner}->next_async( @_ );

}

SYNC_METHODS: {

    *{next} = sync( sub { shift->next_async( @_ ) } );

}

1;