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
use HBase::Client::Try qw( try retry done handle );
use HBase::Client::Context qw( context );

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

sub load_regions {

    my ($self, $table, $region) = @_;

    my $scan = {}; # scan all meta by default

    if (defined $table){

        if ($region){

            context->log( "Loading regions for $table to replace ".$region->name );

            my $end = $region->end;

            $scan = {
                    start_row   => region_name( $table, $region->start ),                             # "$tablename,$start_key,"
                    stop_row    => region_name( $end eq '' ? next_key( $table ) : ( $table, $end ) ), # stop at "$tablename\x00,," if given the last region,
                                                                                                      # otherwise stop at "$tablename,$end_key,"
                };
        } else {

            context->log( "Loading regions for $table" );

            $scan = {
                start_row   => region_name( $table ),             # "$tablename,,"
                stop_row    => region_name( next_key( $table ) ), # "$tablename\x00,,"
            };
        }

    } else {

        context->log( "Loading all regions" );

    }

    my $scanner = $self->table( meta_table_name )->scanner( $scan, { number_of_rows => 1000 } );

    my @regions;

    return try {

            $scanner->next->then( sub {

                    my ($rows) = @_;

                    if ($rows){

                        for my $row (@$rows) {

                            if (my $region = HBase::Client::Region->parse( $self, $row )){

                                next if $region->is_offline || $region->is_split; # these can't be used for serving requests

                                if (my $previous_region = $regions[-1]){

                                    if ($previous_region->table_name eq $region->table_name){

                                        my $end = $previous_region->end;
                                        my $start = $region->start;

                                        warn sprintf( "Bad region sequence: %s ends at %s while the next is %s \n", $previous_region->name, $end, $region->name )
                                            unless $end eq $start;

                                        if ($previous_region->start eq $start){
                                            # This type of inconsistency most likely caused by not closing a region after split.
                                            # We can compensate it taking into account that the region id usually is timestamp of its opening,
                                            # so we just replace the older one.
                                            $regions[-1] = $region;

                                            next;

                                        }

                                    }

                                }

                                push @regions, $region;

                            }

                        }

                        retry( cause => 'Checking for more regions' );

                    }

                });

        }->then( sub {

            if (@regions && $region){

                my $start = $regions[0]->start;
                my $end = $regions[-1]->end;

                unless ($start eq $region->start && $end eq $region->end){

                    context->log( sprintf( "Loading replacing regions failed: %s ends at %s was replaced by a sequence covering $start ... $end \n", $region->name, $region->end, $start, $end) );

                    die sprintf( "Broken regions sequence: %s ends at %s was replaced by a sequence covering $start ... $end \n", $region->name, $region->end, $start, $end);

                }



            }

            context->log( "Loading regions successful" );

            return \@regions;

        } );
}

sub prepare {

    my ($self) = @_;

    # checks and possibly acquires the preparation lock
    return $self->{prepared} //= $self->load_regions
        ->then( sub {

                my ($regions) = @_;

                my %tables;

                push @{ $tables{$_->table_name} //= [] }, $_ for @$regions;

                $self->table( $_ )->inflate( $tables{$_} ) for keys %tables;

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
