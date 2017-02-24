package HBase::Client::Region;

use v5.14;
use warnings;

use HBase::Client::Utils qw( region_specifier );
use Scalar::Util qw( weaken );


sub new {
    my ($class, %args) = @_;

    my $self = bless {
            %args,
        }, $class;

    weaken $self->{cluster};

    return $self;

}

sub get_async {
    my ($self, @args) = @_;

    return $self->_get_node->then( sub {
            my ($node) = @_;

            return $node->get_async( $self->specifier, @args );
        } );

}

sub mutate_async {
    my ($self, @args) = @_;

    return $self->_get_node->then( sub {
            my ($node) = @_;

            return $node->mutate_async( $self->specifier, @args );
        } );

}

sub scan_async {
    my ($self, @args) = @_;

    return $self->_get_node->then( sub {
            my ($node) = @_;

            return $node->scan_async( $self->specifier, @args );
        } );

}

sub scanner {

    my ($self, $scan, $number_of_rows, $exclude_start) = @_;

    return $self->_get_node->then( sub {
            my ($node) = @_;

            return HBase::Client::RegionScanner->new(
                    node                => $node,
                    region              => $self->specifier,
                    scan                => $scan,
                    number_of_rows      => $number_of_rows,
                    exclude_start       => $exclude_start,
                );
        } );
}

sub region_before {

    my ($self) = @_;

    return $self->{cluster}->get_region_before( $self );

}

sub region_after {

    my ($self) = @_;

    return $self->{cluster}->get_region_after( $self );

}

GETTERS: {

    sub _getter {

        my ($property) = @_;

        return sub { $_[0]->{$property} };

    }

    no strict 'refs';

    *{$_} = _getter( $_ ) for qw( name start end server table );

}

sub specifier { region_specifier( shift->name ) }

sub _get_node {
    my ($self) = @_;

    return $self->{cluster}->get_node( $self->server );
}

1;