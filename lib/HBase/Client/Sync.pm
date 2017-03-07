package HBase::Client::Sync;

use v5.14;
use warnings;

use AnyEvent;
use Exporter 'import';

our @EXPORT = qw(
        sync
        call
    );

sub sync {

    my ($sub) = @_;

    return sub {

            my $done = AnyEvent->condvar;

            my ($result,$error);

            $sub->( @_ )
                ->then( sub { $result = shift; }, sub { $error = shift; } )
                ->finally( sub { $done->send; } );

            $done->recv;

            die $error if defined $error;

            return $result;

        };
}

sub call {

    my ($sub, @args) = @_;

    $sub->(@args) if $sub;

    return;
}

1;