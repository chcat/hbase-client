package HBase::Client::MetaTable;

use v5.14;
use warnings;

use parent 'HBase::Client::Table';

use HBase::Client::Utils;
use HBase::Client::Try;
use Promises qw( deferred );
use Scalar::Util qw( weaken );

sub new {
    my ($class, %args) = @_;

    my $self = bless {
            %args,
            name    => meta_table_name,
        }, $class;

    weaken $self->{cluster};

    return $self;

}

sub region {

    my ($self) = @_;

    return $self->{meta_region} //= try {
            $self->{cluster}->{meta_holder_locator}->locate->then( sub {

                    my ($server) = @_;

                    return HBase::Client::Region->new(
                        name        => 'hbase:meta,,1',
                        server      => $server,
                        start       => '',
                        end         => '',
                        table       => $self,
                    );

                }, sub {

                    my ($error) = @_;

                    retry(count => 3, cause => $error);

                } );
        }->catch( sub {

            my ($error) = @_;

            undef $self->{meta_region}; # clear cache of the failed region promise not to leave the client broken

            die 'Unable to locate meta region holder';

        } );

}

sub load { deferred->resolve( undef );}

sub region_after { deferred->resolve( undef ); }

sub region_before { deferred->resolve( undef ); }

1;