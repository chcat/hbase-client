package HBase::Client::Context;

use v5.14;
use warnings;

use Promises;
use Scope::Upper qw( localize SUB UP );

use Exporter 'import';

our @EXPORT_OK= qw(
        context
        set_context
    );

our $context;

our $promise_chainings_wrapped;

sub context {

    return $context;

}

sub set_context {

    my ($new_context) = @_;

    my $old_context = $context;

    if (defined $context != defined $new_context || defined $context && $context != $new_context){

        localize *HBase::Client::Context::context, \$new_context, UP SUB; # UP SUB refers here to the caller of ___SUB___

        unless ($promise_chainings_wrapped){

            localize *Promises::Promise::then, _wrap_promise_chaining( \&Promises::Promise::then, 2 ), UP SUB;

            localize *Promises::Promise::done, _wrap_promise_chaining( \&Promises::Promise::done, 2 ), UP SUB;

            localize *Promises::Promise::catch, _wrap_promise_chaining( \&Promises::Promise::catch, 1 ), UP SUB;

            localize *Promises::Promise::finally, _wrap_promise_chaining( \&Promises::Promise::finally, 1 ), UP SUB;

            localize *HBase::Client::Context::promise_chainings_wrapped, \1, UP SUB;


        }

    }

    return $old_context;

}

sub _wrap_promise_chaining {

    my ($original_sub, $args_amount) = @_;

    return sub {

            my $context = context; # at the moment of chaining a callback to a promise, captures the context into the closure of the callback wrapper defined below

            my $alias_ref = \shift;

            my @callbacks;

            for (my $index = 0; $index < $args_amount; $index++ ){

                my $callback = shift;

                # wraps a promise completion callback by the sub recovering the context from its closure

                push @callbacks, $callback ? sub {

                        set_context( $context );

                        $callback->(@_); # goto would destroy the local context we set up

                    } : undef;

            }

            $original_sub->($$alias_ref, @callbacks, @_);

        };

}

1;