package HBase::Client::RegionServer;

use v5.14;
use warnings;

use Digest::MD5 qw( md5_hex );

use HBase::Client::Connection;
use HBase::Client::Proto::Loader;
use HBase::Client::RPC;

sub new {

    my ($class, %args) = @_;

    my $self = bless {

            rpc     => $args{rpc},

        }, $class;

    return $self;
}

sub get {

    my ($self, $get) = @_;

    my $method = {



        };

    return $self->{rpc}->call( $method, $get );

}

sub _locate_region_in_meta {

    my ($region_name) = @_;

    my $request = HBase::Client::Proto::GetRequest->new( {

            region => _region_specifier( 'hbase:meta,,1' ),

            get    => {

                    row              => $region_name,

                    column           => [ { family => 'info' } ],

                    closest_row_before => 1,

                },

        } );



}

sub _region_specifier {

    my ($region_name) = @_;

    return HBase::Client::Proto::RegionSpecifier->new( {

            type    => HBase::Client::Proto::RegionSpecifier::RegionSpecifierType::REGION_NAME,

            value   => $region_name,

        } );


}

sub _region_name {

    my ($table, $start, $id, $replica_id) = @_;

    my $name = $table . ',' . $start . ',' . $id;

    $name .= '_' . sprintf( '%04X', $replica_id) if $replica_id;

    return $name . '.' . md5_hex( $name );

}

1;