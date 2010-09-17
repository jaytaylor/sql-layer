use `scan_rows_test`;

create table a(
    a0 int not null,
    a1 int not null,
    a2 int not null,
    a3 int not null,
    a4 varchar(64) not null,
    primary key(a1)
) engine = akibadb;

create table aa(
    a1 int not null,
    aa1 int not null,
    aa2 int not null,
    aa3 int not null,
    aa4 varchar(64) not null,
    primary key(aa1),
    key (`aa2`, `aa3`),
    key (`aa2`, `aa1`),
    key str (aa4),
CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`a1`) REFERENCES `a` (`a1`)
) engine = akibadb;

create table aaa(
    aa1 int not null,
    aaa1 int not null,
    aaa2 int not null,
    aaa3 int not null,
    aaa4 varchar(64) not null,
    primary key(aaa1),
    key (`aaa1`),
    key (`aaa2`),
    key (`aaa3`),
CONSTRAINT `__akiban_fk_1` FOREIGN KEY `__akiban_fk_1` (`aa1`) REFERENCES `aa` (`aa1`)
) engine = akibadb;

create table aaaa(
    aaa1 int not null,
    aaaa1 int not null,
    aaaa2 int not null,
    aaaa3 int not null,
    aaaa4 varchar(64) not null,
    primary key(aaaa1),
    key (`aaaa1`),
    key (`aaaa2`),
    key (`aaaa3`)    ,
CONSTRAINT `__akiban_fk_2` FOREIGN KEY `__akiban_fk_2` (`aaa1`) REFERENCES `aaa` (`aaa1`)
) engine = akibadb;

create table aaaaa(
    aaaa1 int not null,
    aaaaa1 int not null,
    aaaaa2 int not null,
    aaaaa3 int not null,
    aaaaa4 varchar(64) not null,
    primary key(aaaaa1),
CONSTRAINT `__akiban_fk_3` FOREIGN KEY `__akiban_fk_3` (`aaaa1`) REFERENCES `aaaa` (`aaaa1`)
) engine = akibadb;

create table aaaab(
    aaaa1 int not null,
    aaaab1 int not null,
    aaaab2 int not null,
    aaaab3 int not null,
    aaaab4 varchar(64) not null,
    primary key(aaaab1),
CONSTRAINT `__akiban_fk_4` FOREIGN KEY `__akiban_fk_4` (`aaaa1`) REFERENCES `aaaa` (`aaaa1`)
) engine = akibadb;

create table aaab(
    aaa1 int not null,
    aaab1 int not null,
    aaab2 int not null,
    aaab3 int not null,
    aaab4 varchar(64) not null,
    primary key(aaab1),
    key (`aaab1`, `aaab2`),
    key (`aaab2`, `aaab3`),
    key aaab3aaab1 (`aaab3`, `aaab1`),
    key (`aaab1`, `aaab3`),
    key (`aaab2`, `aaab1`),
CONSTRAINT `__akiban_fk_5` FOREIGN KEY `__akiban_fk_5` (`aaa1`) REFERENCES `aaa` (`aaa1`)
) engine = akibadb;

create table aab(
    aa1 int not null,
    aab1 int not null,
    aab2 int not null,
    aab3 int not null,
    aab4 varchar(64) not null,
    primary key(aab1),
CONSTRAINT `__akiban_fk_6` FOREIGN KEY `__akiban_fk_6` (`aa1`) REFERENCES `aa` (`aa1`)
) engine = akibadb;

create table aac(
    aa1 int not null,
    aac1 int not null,
    aac2 int not null,
    aac3 int not null,
    aac4 varchar(64) not null,
    primary key(aac1),
CONSTRAINT `__akiban_fk_7` FOREIGN KEY `__akiban_fk_7` (`aa1`) REFERENCES `aa` (`aa1`)
) engine = akibadb;

create table ab(
    a1 int not null,
    ab1 int not null,
    ab2 int not null,
    ab3 int not null,
    ab4 varchar(64) not null,
    primary key(ab1),
CONSTRAINT `__akiban_fk_8` FOREIGN KEY `__akiban_fk_8` (`a1`) REFERENCES `a` (`a1`)
) engine = akibadb;