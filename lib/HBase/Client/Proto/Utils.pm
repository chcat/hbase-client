package HBase::Client::Proto::Utils;

use strict;
use warnings;

use Exporter 'import';
our @EXPORT= ('read_varint', 'encode_varint');

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