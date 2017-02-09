package HBase::Client::ClusterScanner;

use v5.14;
use warnings;

use HBase::Client::Utils;
use HBase::Client::Try;

sub new {

    my ($class, %args) = @_;

    my $self = bless {
            %args,
            next_call_seq => 0,
            start_row     => $args{scan}->{start_row} // '',
            stop_row      => $args{scan}->{stop_row},
        }, $class;

    return $self;

}

sub next_async {

    my ($self) = @_;

    try( sub {

            my $scanner = $self->{scanner} //= $self->_get_region_scanner;

            $scanner
                ->then( sub {

                        return shift->next_async;

                    } )
                ->catch( sub {

                        my $error = (@_);

                        if (exception($error) eq 'org.apache.hadoop.hbase.UnknownScannerException' ){

                            retry;

                        } else {

                            die $error;

                        }

                    } );

        } );

}

sub _get_region_scanner {

    my ($self) = @_;

    return $self->{cluster}->_get_region_and_node( $self->{table}, $self->{start_row} )
        ->then( sub {

                my ($region, $node) = @_;

                return $node->scan( $region, $self->{scan}, $self->{number_of_rows} );

            } );

}


1;