package HBase::Client::Table;

use v5.14;
use warnings;

use HBase::Client::Utils;
use List::BinarySearch qw( binsearch_pos );
use Scalar::Util qw( weaken );

sub new {
    my ($class, %args) = @_;

    my $self = bless {
            %args,
            regions => [],
        }, $class;

    weaken $self->{cluster};

    return $self;

}

sub region {

    my ($self, $row) = @_;

    my $regions = $self->{regions};

    my $position = binsearch_pos { ($b->start le $a && ($b->end gt $a || $b->end eq '')) ? 0 : $a <=> $b->start } $row, @$regions;

    if ($position < @$regions){

        return $regions->[$position];

    } else {

        $self->_create_table_map;

        return undef;

    }

}

sub _create_table_map {

    my ($self) = @_;

    return if $self->{mapping};

    $self->{mapping} = 1;

    my $table = $self->name;

    my $scan = {
            start_row   => region_name( $table ),
            stop_row    => region_name( next_key( $table ) ),
        };

    my $cluster = $self->{cluster};

    my $scanner = $cluster->scanner( meta_table_name, $scan, 1000 );

    my $regions = $self->{regions} = [];

    try {

        $scanner->next->then( sub {

                my ($response) = @_;

                if ($response){

                    push @$regions, $cluster->region_from_row($_) for @{$response->get_results_list // []};

                    retry( cause => 'Check for more regions' );

                } else {

                    undef $self->{mapping};

                }

            });

    }

}

GETTERS: {

    sub _getter {

        my ($property) = @_;

        return sub { $_[0]->{$property} };

    }

    no strict 'refs';

    *{$_} = _getter( $_ ) for qw( name );

}

1;