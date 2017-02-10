package HBase::Client::TableScanner;

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

    try {

        my $scanner = $self->{scanner} //= $self->_get_region_scanner;

        return $scanner
            ->then( sub {

                    return shift->next_async;

                } )
            ->then( sub {

                    my ($response) = @_;

                    $self->{start_row} = next_key( $response->get_results( $response->results_size - 1 )->get_cell(0)->get_row );

                    return $response;

                }, sub {

                    my ($error) = @_;

                    if (exception($error) eq 'org.apache.hadoop.hbase.UnknownScannerException' ){

                        undef $self->{scanner};

                        retry( cause => $error );

                    } else {

                        die $error;

                    }

                } );


        };

}

sub _get_region_scanner {

    my ($self) = @_;

    $self->{scan}->{start_row} = $self->{start_row};

    return $self->{cluster}->_get_region_and_node( $self->{table}, $self->{start_row} )
        ->then( sub {

                my ($region, $node) = @_;

                return $node->scan( $region, $self->{scan}, $self->{number_of_rows} );

            } );

}


1;