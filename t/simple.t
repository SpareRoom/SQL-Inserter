use Test2::V0;
use Test2::Mock;

use SQL::Inserter;

my @prepare = ();
my @execute = ();

my $dbh = mock {} => (
    add => [
        prepare => sub { my $self = shift; push @prepare, @_ ; return $self},
        execute => sub { shift; @execute = @_ ; return 1},
    ]
);

subtest 'single_buffer' => sub {
    my $sql = SQL::Inserter->new(dbh=>$dbh,table=>'table',cols=>[qw/col1 col2/],buffer=>1);

    $sql->insert(1,2);
    is([@prepare],["INSERT INTO table (col1,col2)\nVALUES (?,?);"], "Prepared statement");
    is($sql->{last_retval}, 1, "execute");
    is($sql->{row_total}, 1, "row_total");
    is([@execute],[1,2], "Bind variables correct");

    $sql->insert(1,2,3,4);
    is(scalar(@prepare),1, "Reused prepared statement");
    is([@execute],[3,4], "Last execute bind vars");
    is($sql->{row_total}, 3, "New row_total");

    $sql->insert(); ## noop
    $sql->insert({col1=>'a'});
    is([$prepare[1]],["INSERT INTO table (col1,col2)\nVALUES (?,?);"], "New prepared statement");
    is([@execute],['a',undef], "Bind variables correct");
    is($sql->{row_total}, 4, "New row_total");
};

subtest 'duplicates' => sub {
    @prepare = ();
    my $sql = SQL::Inserter->new(dbh=>$dbh,table=>'table',cols=>[qw/col1 col2/],buffer=>1, duplicates=>'ignore');
    $sql->insert(1,2);
    is([@prepare],["INSERT IGNORE INTO table (col1,col2)\nVALUES (?,?);"], "Prepared statement");
    is([@execute],[1,2], "Bind variables correct");

    @prepare = ();
    $sql = SQL::Inserter->new(dbh=>$dbh,table=>'table',cols=>[qw/col1 col2/],buffer=>1, duplicates=>'update');
    $sql->insert(1,2);
    is([@prepare],["INSERT INTO table (col1,col2)\nVALUES (?,?)\nON DUPLICATE KEY UPDATE col1=VALUES(col1),col2=VALUES(col2);"], "Prepared statement");
    is([@execute],[1,2], "Bind variables correct");
};

subtest 'null_undef' => sub {
    @prepare = ();
    my $sql = SQL::Inserter->new(dbh=>$dbh,table=>'table',cols=>[qw/col1 col2/],buffer=>1, null_undef=>1);
    $sql->insert(1,undef);
    is([@prepare],["INSERT INTO table (col1,col2)\nVALUES (?,?);"], "Prepared statement");
    is([@execute],[1,undef], "Bind variables correct");

    $sql->insert({col2=>1});
    is([$prepare[1]],["INSERT INTO table (col1,col2)\nVALUES (NULL,?);"], "New prepared statement");
    is([@execute],[1], "Bind variables correct");
};

subtest 'multi_buffer' => sub {
    @prepare = ();
    my $sql = SQL::Inserter->new(dbh=>$dbh,table=>'table',cols=>[qw/col1 col2/],buffer=>3);
    $sql->insert(1,2);
    is([@prepare],[], "No prepared statement");
    is($sql->{row_total}, undef, "No row_total");

    $sql->insert(1..12);
    is([@prepare],["INSERT INTO table (col1,col2)\nVALUES (?,?),(?,?),(?,?);"], "Single prepared statement");
    is([@execute],[5..10], "Last execute bind vars");
    is($sql->{row_total}, 2, "Two executes");
    is($sql->{bind}, [11,12], "Two left");

    $sql->insert();
    is($prepare[1],"INSERT INTO table (col1,col2)\nVALUES (?,?);", "Empty buffer prepared statement");
    is([@execute],[11,12], "Last execute bind vars");
    is($sql->{row_total}, 3, "3 executes");

    @prepare = ();
    $sql->insert({});
    $sql->insert({});
    is([@prepare],[], "No prepared statement");
    $sql->insert({col1=>\"NULL"});
    is([@prepare],["INSERT INTO table (col1,col2)\nVALUES (?,?),\n(?,?),\n(NULL,?);"], "Prepared statement");
    is([@execute],[(undef) x 5], "Last execute bind vars");
    is($sql->{row_total}, 4, "4 executes");
    $sql->insert({});
    $sql->insert({});
    $sql->insert({});
    $sql->insert({col2=>\"NOW()"});
    is($prepare[1],"INSERT INTO table (col1,col2)\nVALUES (?,?),\n(?,?),\n(?,?);", "New prepared statement");
    is([@execute],[(undef) x 6], "Last execute bind vars");
    is($sql->{row_total}, 5, "6 executes");
    $sql = undef;
    is($prepare[2],"INSERT INTO table (col1,col2)\nVALUES (?,NOW());", "New prepared statement on destroy");
    is([@execute],[undef], "Last execute bind vars on destroy");

    @prepare = ();
    {
        my $sql = SQL::Inserter->new(dbh=>$dbh,table=>'table',cols=>[qw/col/]);
        $sql->insert(1..102);
        is([@execute],[1..100], "Bind vars");
    }
    is([@execute],[101,102], "Last execute bind vars on destroy");
};

done_testing;