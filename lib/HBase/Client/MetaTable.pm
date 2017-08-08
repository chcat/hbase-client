package HBase::Client::MetaTable;

use v5.14;
use warnings;

use parent 'HBase::Client::Table';

use HBase::Client::Utils;
use HBase::Client::Try qw( try retry done handle );
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

    my ($self, $row, $offset) = @_;

    return deferred->resolve( undef ) if $offset; # there is a single region in the meta table

    return $self->{meta_region} //= try {
            $self->cluster->meta_server->then( sub {

                    my ($server) = @_;

                    return HBase::Client::Region->new(
                        name        => 'hbase:meta,,1',
                        server      => $server,
                        start       => '',
                        end         => '',
                        table_name  => $self->name,
                        cluster     => $self->cluster,
                    );

                }, sub {

                    my ($error) = @_;

                    warn sprintf("Error locating meta table holder: %s \n", $error);

                    retry(count => 3, cause => 'Meta table locating error');

                } );
        }->catch( sub {

            my ($error) = @_;

            undef $self->{meta_region}; # clear cache of the failed region promise not to leave the client broken

            die 'Unable to locate meta region holder';

        } );

}

sub invalidate {

    my ($self) = @_;

    undef $self->{meta_region};

    return $self->region;

}

sub load {
    my ($self) = @_;

    return $self->region->then( sub {

            return [$_[0]];

        } );

}

1;