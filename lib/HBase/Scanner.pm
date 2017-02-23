package HBase::Scanner;

use v5.14;
use warnings;

use HBase::Client::Sync;

sub new {

    my ($class, %args) = @_;

    return bless {
            client  => $args{client},
            scanner => $args{client}->_cluster->scanner( $args{table}, $args{scan}, $args{number_of_rows} // 1000 ),
        }, $class;

}

sub next_async {

    return shift->{scanner}->next_async( @_ );

}

SYNC_METHODS: {

    *{next} = sync( sub { shift->next_async( @_ ) } );

}

1;