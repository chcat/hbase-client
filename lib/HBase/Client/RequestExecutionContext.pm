package HBase::Client::RequestExecutionContext;

use v5.14;
use warnings;

use Time::HiRes qw( time );

sub new {

    my ($class, $self) = @_;

    $self //= {};

    $self->{log} = [];
    $self->{queries} = [];

    return bless $self, $class;

}

sub log {

    my ($self, $message) = @_;

    push @{$self->{log}}, { time => time, message => $message };

    return;

}

sub region_query_success {

    shift->region_query(1, @_);

}

sub region_query_failure {

    shift->region_query(0, @_);

}

sub region_query {

    my ($self, $result, $region, $query) = @_;

    push @{$self->{queries}}, {
            region => $region->name,
            table  => $region->table_name,
            type   => $query->type,
            result => $result,
        };

    return;


}

1;