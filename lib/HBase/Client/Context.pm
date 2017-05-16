package HBase::Client::Context;

use v5.14;
use warnings;

use Promises;
use Scope::Upper qw( localize );

use Exporter 'import';

our @EXPORT_OK= qw(
        context
        setup_context
    );

my $current_context;

sub context { return $current_context }

sub setup_context {

    my ($class, $context) = @_;

    return if $current_context == $context;

    localize $current_context, $context;

    localize *Promises::then, $class->_wrap_chaining( \&Promises::then, 2 );

    localize *Promises::done, $class->_wrap_chaining( \&Promises::done, 2 );

    localize *Promises::catch, $class->_wrap_chaining( \&Promises::catch, 1 );

    localize *Promises::finally, $class->_wrap_chaining( \&Promises::finally, 1 );

}

sub _wrap_chaining {

    my ($class, $original_sub, $args_amount) = @_;

    my $context = $current_context; # make it closure

    return sub {

            my $self_alias_ref = \$_[0];

            shift;

            my @callbacks;

            for (my $index = 0; $index < $args_amount; $index++ ){

                my $callback = shift;

                push @callbacks, $callback ? sub {

                        $class->setup( $context );

                        goto $callback;

                    } : undef;

            }

            $original_sub->($$self_alias_ref, @callbacks, @_);

        };

}

1;