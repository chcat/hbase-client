package HBase::Client::Proto::Utils;

use strict;
use warnings;

sub read_varint {

    my ( $buf_ref, $advance ) = @_;

    $advance = 1 unless defined $advance;

    my $v = 0;

    my $cnt = 0;

    my $limit = length $$buf_ref;

    my $b;

    do {

        die "Unexpected end of the buffer" if $cnt >= $limit;

        die "Varint value is too big" if $cnt > 8;

        $b = unpack ("C", substr $$buf_ref, $cnt, 1);

        $v |= ( ( $b & 0x7F ) << $cnt * 7 );

        $cnt ++;

    } while ( $b & 0x80 );

    substr $$buf_ref, 0, $cnt, '' if $advance;

    return $v;

}

1;