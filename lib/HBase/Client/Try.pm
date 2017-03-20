package HBase::Client::Try;

use v5.14;
use warnings;

use AnyEvent;
use Promises qw( deferred );
use Scalar::Util qw( blessed );

use Exporter 'import';

our @EXPORT= qw(
        try
        retry
        done
        delay
        catchable
    );

sub retry {

    die HBase::Client::Try::Retry->new( @_ );

}

sub done {

    die HBase::Client::Try::Done->new( @_ );

}

sub try (&) {

    my ($sub) = @_;

    my $deferred = deferred;

    my $state = {
            count => 0,
        };

    _try_loop( $sub, $deferred, $state );

    return $deferred->promise;

}

sub delay ($&) {

    my ($delay, $sub) = @_;

    my $deferred = deferred;

    state $timers = {};
    state $timers_count = 0;

    my $timer = $timers_count++;

    AnyEvent->now_update;

    $timers->{$timer} = AnyEvent->timer(
            after => $delay,
            cb => sub {

                    delete $timers->{$timer};

                    $deferred->resolve();

                }
        );

    return $deferred->promise->then( $sub );

}

sub handle {

    my ($error) = @_;

    my $error_type = blessed $error // '';

    die $error if $error_type eq 'HBase::Client::Try::Done' or $error_type eq 'HBase::Client::Try::Retry';

    return;

}

sub _try_loop {

    my ($sub, $deferred, $state ) = @_;

    $sub->()->done( sub {

            $deferred->resolve( @_ );

        }, sub {

            my ($error) = @_;

            my $error_type = blessed $error // '';

            if ($error_type eq 'HBase::Client::Try::Done'){

                $deferred->resolve( @$error );

                return;

            } elsif ($error_type eq 'HBase::Client::Try::Retry'){

                my $retry_attempt = $state->{count}++; # 0 ..

                if (my $delays = $error->{delays}){

                    if ($retry_attempt < @$delays){

                        my $delay = $delays->[$retry_attempt];

                        return delay $delay, sub {_try_loop( $sub, $deferred, $state )};

                    }

                } else {

                    my $limit = $error->{count};

                    if (!defined $limit or $retry_attempt < $limit){

                        return _try_loop( $sub, $deferred, $state ) ;

                    }

                }

                $error = $error->{cause};

            }

            $deferred->reject( $error );

            return;

        } );

}

package HBase::Client::Try::Retry;

use v5.14;
use warnings;

sub new { bless { @_[1..$#_] }, $_[0]; }

package HBase::Client::Try::Done;

use v5.14;
use warnings;

sub new { bless [ @_[1..$#_] ], $_[0]; }

1;