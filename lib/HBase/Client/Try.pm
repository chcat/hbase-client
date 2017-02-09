package HBase::Client::Try;

use v5.14;
use warnings;

use Promises qw( deferred );
use Exporter 'import';

our @EXPORT= qw(
        try
        retry
    );

use constant RETRY => \0;

sub retry {

    die RETRY;

}

sub try {

    my ($sub) = @_;

    my $deferred = deferred;

    _try_loop( $sub, $deferred );

    return $deferred->promise;

}

sub _try_loop {

    my ($sub, $deferred ) = @_;

    $sub->()->done( sub {

            $deferred->resolve( @_ );

        }, sub {

            my ($error) = @_;

            if ($error == RETRY){

                $self->_try_loop( $sub, $deferred );

            } else {

                $deferred->reject( $error );

            }

        } );

}

1;