package HBase::Client::Proto::Loader;

use strict;
use warnings;

use File::Basename;
use Google::ProtocolBuffers::Dynamic;

my $initialized //= do {

    my $dir = dirname( __FILE__ ).'/protobuf/';

    # reverse topological ordering
    my @files = qw (
            Cell.proto
            HBase.proto
            ClusterId.proto
            FS.proto
            ClusterStatus.proto
            ZooKeeper.proto
            Tracing.proto
            RPC.proto
            Comparator.proto
            Filter.proto
            Client.proto
        );

    my $dynamic = Google::ProtocolBuffers::Dynamic->new;

    foreach my $file ( @files ){

        # as $dynamic->load_file does not work somehow, we read proto definitions manually

        open my $fh, '<', $dir.$file or die "can't open $file: $!";

        read( $fh, my $proto, -s $fh );

        $dynamic->load_string( $file, $proto );

    }

    $dynamic->map({ package => '', prefix => 'HBase::Client::Proto' });

    1;

};