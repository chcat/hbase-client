package HBase::Client::Cluster;

use v5.14;
use warnings;

use HBase::Client::Table;
use HBase::Client::MetaTable;
use HBase::Client::Region;
use HBase::Client::Utils qw(
        next_key
        region_name
        meta_table_name
    );

sub new {

    my ($class, %args) = @_;

    my $meta_holder_locator = $args{meta_holder_locator};

    my $node_pool = $args{node_pool};

    my $self = bless {
            meta_holder_locator => $meta_holder_locator,
            node_pool           => $node_pool,
            tables              => {},
        }, $class;

    my $meta_table = HBase::Client::MetaTable->new( cluster => $self );

    $self->{tables}->{ $meta_table->name } = $meta_table;

    return $self;

}

sub meta_server {

    my ($self) = @_;

    return $self->{meta_holder_locator}->locate;

}

sub get_node {

    my ($self, $server) = @_;

    return $self->{node_pool}->get_node( $server );

}

sub table {

    my ($self, $table_name) = @_;

    return $self->{tables}->{$table_name} //= HBase::Client::Table->new(
            cluster     => $self,
            name        => $table_name,
        );

}

sub load_online_regions {

    my ($self, $table) = @_;

    my $scan = defined $table ? {
            start_row   => region_name( $table ),              # "$tablename,,"
            stop_row    => region_name( next_key( $table ) ), # "$tablename\x00,,"
        } : {};

    my $scanner = $self->table( meta_table_name )->scanner( $scan, { number_of_rows => 1000 } );

    my @regions;

    return try {

            $scanner->next->then( sub {

                    if (my $rows = $_[0]){

                        for my $row (@$rows) {

                            my $region = HBase::Client::Region->parse( $self, $row );

                            push @regions, $region unless $region->is_offline;

                        }

                        retry( cause => 'Checking for more regions' );

                    }

                });

        }->then( sub {

            return \@regions;

        } );
}

sub prepare {

    my ($self) = @_;

    # checks and possibly acquires the preparation lock
    return $self->{prepared} //= $self->load_online_regions
        ->then( sub {

                my ($regions) = @_;

                my %tables;

                push @{ $tables{$_->table_name} //= [] }, $_ for @$regions;

                $self->table( $_ )->load( $tables{$_} ) for keys %tables;

            }, sub {

                my ($error) = @_;

                die "Loading regions failed: $error" ;

            } )
        ->finally( sub {

                $self->{node_pool}->disconnect;

                undef $self->{prepared}; # releases the preparation lock

            } );

}

sub shutdown {

    my ($self) = @_;

    return $self->{node_pool}->shutdown;

}

1;
