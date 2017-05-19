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

sub region_query_start {

    my ($self, $region, $query) = @_;

    push @{$self->{queries}}, {
            region => $region->name,
            table  => $region->table_name,
            type   => $query->type,
            time   => time,
        };

}

sub region_query_success { shift->_region_query_end(1); }

sub region_query_failure { shift->_region_query_end(0); }

sub _region_query_end {

    my ($self, $result) = @_;

    my $query = $self->{queries}[-1];

    $query->{result} = $result;
    $query->{time} = int (time - $query->{time})*1000;

}

sub register_io_stats {

    my ($self, $stats) = @_;

    if (my $query = $self->{queries}[-1]){

        $query->{io} = { %$stats };

    }

}





1;