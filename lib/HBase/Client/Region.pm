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

    weaken $self->{table};

    return $self;

}

sub get_async {
    my ($self, @args) = @_;

    return $self->_get_node->get_async( $self->_specifier, @args );

}

sub mutate_async {
    my ($self, @args) = @_;

    return $self->_get_node->mutate_async( $self->_specifier, @args );

}

sub scan_async {
    my ($self, @args) = @_;

    return $self->_get_node->scan_async( $self->_specifier, @args );

}

sub scanner {

    my ($self, $scan, $number_of_rows) = @_;

    return HBase::Client::RegionScanner->new(
            region              => $self,
            scan                => $scan,
            number_of_rows      => $number_of_rows,
        );

}

sub region_before {

    my ($self) = @_;

    return $self->table->region_before( $self );

}

sub region_after {

    my ($self) = @_;

    return $self->table->region_after( $self );

}

GETTERS: {

    sub _getter {

        my ($property) = @_;

        return sub { $_[0]->{$property} };

    }

    no strict 'refs';

    *{$_} = _getter( $_ ) for qw( name start end server table );

}

sub _specifier { region_specifier( $_[0]->name ) }

sub _get_node {
    my ($self) = @_;

    return $self->{table}->cluster->get_node( $self->server );
}

1;