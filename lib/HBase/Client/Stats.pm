package HBase::Client::Try;

use v5.14;
use warnings;

use AnyEvent;
use Promises qw( deferred );
use Scalar::Util qw( blessed );

use Exporter 'import';

our @EXPORT_OK= qw(
        try
        retry
        done
        delay
        handle
        sync
        timeout
    );

sub record ($&) {

    my ($timeout, $sub) = @_;

    return $sub->() unless ($timeout // 0) > 0;

    my $deferred = deferred;

    state $timers = {};
    state $timers_count = 0;

    my $timer = $timers_count++;

    AnyEvent->now_update;

    $timers->{$timer} = AnyEvent->timer(
            after => $timeout,
            cb => sub {

                    delete $timers->{$timer};

                    $deferred->reject('TIMEOUT');

                }
        );

    $sub->()
        ->finally( sub {

                delete $timers->{$timer};

            } )
        ->then( sub {

                $deferred->resolve(@_);

            }, sub {

                $deferred->reject(@_);

            } );

    return $deferred->promise;

}

1;