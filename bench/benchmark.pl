#!/usr/bin/env perl

use strict;
use warnings;
use lib "lib";

use Benchmark 'cmpthese';

use SQL::Abstract;
use SQL::Inserter;
use SQL::Maker;

my $sql_abstract = SQL::Abstract->new();
my $sql_maker = SQL::Maker->new(driver => 'mysql');

my @data = create_data();

cmpthese -2, {
    abstract => sub {
        my ($stmt, @bind) = $sql_abstract->insert('data_table', data());
    },
    maker => sub {
        my ($stmt, @bind) = $sql_maker->insert('data_table', data());
    },
    abstract_new => sub {
        my $sql = SQL::Abstract->new();
        my ($stmt, @bind) = $sql->insert('data_table', data());
    },
    maker_new => sub {
        my $sql = SQL::Maker->new(driver => 'mysql');
        my ($stmt, @bind) = $sql->insert('data_table', data());
    },
    simple_insert => sub {
        my ($stmt, @bind) = SQL::Inserter::simple_insert('data_table', data());
    },
};


sub create_data {
    my @data;
    foreach (1..10) {
        my $d = {
        id   => int(rand(10000000)),
        date => \"NOW()",
        map {"data".$_ => "foo bar" x int(rand(5)+1)} 1..int(rand(20)+1)
        };
        push @data, $d;
    }
    return @data;
}



sub data {
    return $data[int(rand(10))];
    return {
        id    => int(rand(10000000)) + 1,
        data1 => "foo",
        data2 => "bar",
        data3 => "foobar",
        data10 => \"NOW()",
        data20 => "bar",
        time  => time()
    };
}
