package HBase::Client::Proto::Utils;

use strict;
use warnings;

use Exporter 'import';
our @EXPORT= ('split_delimited','join_delimited');

sub split_delimited {

    my ($buffer_reference) = @_;

    my @pieces;

    push @pieces, substr( $$buffer_reference, 0, read_varint( $buffer_reference ), '' ) while length $$buffer_reference;

    return \@pieces;
}

sub join_delimited {

    my ( $pieces ) = @_;

    my $buffer = join '', map { ( encode_varint( length $_ ), $_ ) } @$pieces;

    return \$buffer;

}

sub read_varint {

    my ($buffer_reference) = @_;

    my $value = 0;

    my $byte_count = 0;

    my $limit = length $$buffer_reference;

    my $byte;

    do {

        die "Unexpected end of the buffer" if $byte_count >= $limit;

        die "Varint value is too big" if $byte_count > 8;

        $byte = unpack ("C", substr ($$buffer_reference, $byte_count, 1));

        $value |= ( ( $byte & 0x7F ) << $byte_count * 7 );

        $byte_count ++;

    } while ( $byte & 0x80 );

    substr $$buffer_reference, 0, $byte_count, '';

    return $value;

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