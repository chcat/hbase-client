package HBase::Client::Proto::Utils;

use strict;
use warnings;

use Exporter 'import';
our @EXPORT= ('read_varint', 'encode_varint');

sub read_varint {

    my $advance = $_[1] // 1;

    my $v = 0;

    my $cnt = 0;

    my $limit = length $_[0];

    my $b;

    do {

        die "Unexpected end of the buffer" if $cnt >= $limit;

        die "Varint value is too big" if $cnt > 8;

        $b = unpack ("C", substr $_[0], $cnt, 1);

        $v |= ( ( $b & 0x7F ) << $cnt * 7 );

        $cnt ++;

    } while ( $b & 0x80 );

    substr $_[0], 0, $cnt, '' if $advance;

    return $v;

}

sub encode_varint {

    my ( $v ) = @_;

    my $r = '';

    while ( $v >= 0x80 ){

        $r .= pack ( "C", $v & 0x7F | 0x80);

        $v >>= 7;

    }

    $r .= pack ( "C", $v );

    return $r;

}

1;