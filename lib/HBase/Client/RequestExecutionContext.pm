package HBase::Client::RequestExecutionContext;

use v5.14;
use warnings;

use Time::HiRes qw( time );

sub log {

    my ($self, $message) = @_;

    push @{$self->{log}}, { time => time, message => $message };

    return;

}

sub new {

    my ($class, $holder) = @_;

    $holder //= {};

    $holder->{log} = [];

    return bless $holder, $class;

}


1;