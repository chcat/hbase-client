package HBase::Client::Context;

use v5.14;
use warnings;

use Promises;
use Scope::Upper qw( localize UP );

use Exporter 'import';

our @EXPORT_OK= qw(
        context
        setup_context
    );

our $context;

our $promise_chainings_wrapped;

sub context { return $context // {}; }

sub setup_context {

    my ($new_context) = @_;

    return if defined $context && defined $new_context && $context == $new_context || !defined $context && !defined $new_context;

    localize *HBase::Client::Context::context, \$new_context, UP;

    return if $promise_chainings_wrapped;

    localize *Promises::Promise::then, _wrap_promise_chaining( \&Promises::Promise::then, 2 ), UP;

    localize *Promises::Promise::done, _wrap_promise_chaining( \&Promises::Promise::done, 2 ), UP;

    localize *Promises::Promise::catch, _wrap_promise_chaining( \&Promises::Promise::catch, 1 ), UP;

    localize *Promises::Promise::finally, _wrap_promise_chaining( \&Promises::Promise::finally, 1 ), UP;

    localize *HBase::Client::Context::promise_chainings_wrapped, \1, UP;

    return;

}

sub _wrap_promise_chaining {

    my ($original_sub, $args_amount) = @_;

    return sub {

            my $context = context; # capture the context at the moment of chaining a callback to a promise into the closure of the callback wrapper

            my $alias_ref = \shift;

            my @callbacks;

            for (my $index = 0; $index < $args_amount; $index++ ){

                my $callback = shift;

                push @callbacks, $callback ? sub {

                        setup_context( $context );

                        $callback->(@_); # goto would destroy the local context we set up

                    } : undef;

            }

            $original_sub->($$alias_ref, @callbacks, @_);

        };

}

1;