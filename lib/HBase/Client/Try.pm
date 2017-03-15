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

    $timers->{$timer} = AnyEvent->timer(
            after => $delay,
            cb => sub {

                    delete $timers->{$timer};

                    $deferred->resolve();

                }
        );

    return $deferred->promise->then( $sub );

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

                $state->{count}++;

                if (!defined $error->{count} || $error->{count} >= $state->{count}){

                    if (my $delay = $error->{delay}){

                        return delay $delay, {_try_loop( $sub, $deferred, $state );};

                    } else {

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