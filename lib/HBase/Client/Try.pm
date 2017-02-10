package HBase::Client::Try;

use v5.14;
use warnings;

use Promises qw( deferred );
use Scalar::Util qw( blessed );
use Exporter 'import';

our @EXPORT= qw(
        try
        retry
    );

sub retry {

    die HBase::Client::Try::Retry->new( @_ );

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

sub _try_loop {

    my ($sub, $deferred, $state ) = @_;

    $sub->()->done( sub {

            $deferred->resolve( @_ );

        }, sub {

            my ($error) = @_;

            if ((blessed $error // '') eq 'HBase::Client::Try::Retry'){

                $state->{count}++;

                if (!defined $error->{count} || $error->{count} >= $state->{count}){

                    return _try_loop( $sub, $deferred, $state ) ;

                }

                $error = $error->{cause};

            }

            $deferred->reject( $error );

        } );

}

package HBase::Client::Try::Retry;

use v5.14;
use warnings;

sub new { bless { @_[1..$#_] }, $_[0]; }

1;