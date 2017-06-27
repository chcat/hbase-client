package HBase::Client::Error;

use v5.14;
use warnings;

use Ref::Util qw( is_blessed_ref );

use Exporter 'import';

our @EXPORT_OK = qw(
        connection_error
        timeout_error
        exception_error
        region_error
        is_connection_error
        is_timeout_error
        is_exception_error
        is_region_error
    );

sub connection_error { HBase::Client::Error::Connection->new( @_ ); }
sub timeout_error { HBase::Client::Error::Timeout->new( @_ ); }
sub exception_error { HBase::Client::Error::Exception->new( @_ ); }
sub region_error { HBase::Client::Error::Region->new( @_ ); }

sub is_connection_error { is_blessed_ref($_[0]) && $_[0]->isa('HBase::Client::Error::Connection') }
sub is_timeout_error { is_blessed_ref($_[0]) && $_[0]->isa('HBase::Client::Error::Timeout') }
sub is_exception_error { is_blessed_ref($_[0]) && $_[0]->isa('HBase::Client::Error::Exception') }
sub is_region_error { is_blessed_ref($_[0]) && $_[0]->isa('HBase::Client::Error::Region') }

sub new {

    my ($class, $cause) = @_;

    return bless {

            cause => $cause,

        }, $class;

}

sub cause { $_[0]->{cause}; }

package HBase::Client::Error::Connection;

use v5.14;
use warnings;

use parent 'HBase::Client::Error';

package HBase::Client::Error::Timeout;

use v5.14;
use warnings;

use parent 'HBase::Client::Error';

package HBase::Client::Error::Exception;

use v5.14;
use warnings;

use parent 'HBase::Client::Error';

sub exception_class {

    my ($self) = @_;

    my $exception = $self->cause;

    return is_blessed_ref( $exception ) && $exception->isa( 'HBase::Client::Proto::ExceptionResponse' ) ? $exception->get_exception_class_name : 'unknown';

}

sub exception { $_[0]->cause }

package HBase::Client::Error::Region;

use v5.14;
use warnings;

use parent 'HBase::Client::Error';

1;