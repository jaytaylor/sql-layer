
/*
schema scan_rows_test;

group srt {
  table a {
    table aa (a1) {
      table aaa (aa1) {
      	table aaaa (aaa1) {
          table aaaaa (aaaa1),
          table aaaab (aaaa1)
        },
        table aaab (aaa1)
      },
      table aab (aa1),
      table aac (aa1)
    },
    table ab (a1)
  }
};

*/


create table a(
    a0 int not null,
    a1 int not null,
    a2 int not null,
    a3 int not null,
    primary key(a1)
) engine = akibadb;
 
create table aa(
    a1 int not null,
    aa1 int not null,
    aa2 int not null,
    aa3 int not null,
    primary key(aa1),
    key (`aa2`, `aaa3`),
    key (`aa2`, `aa1`)
) engine = akibadb;
 
create table aaa(
    aa1 int not null,
    aaa1 int not null,
    aaa2 int not null,
    aaa3 int not null,
    primary key(aaa1),
    key (`aaa1`),
    key (`aaa2`),
    key (`aaa3`)
) engine = akibadb;
 
create table aaaa(
    aaa1 int not null,
    aaaa1 int not null,
    aaaa2 int not null,
    aaaa3 int not null,
    primary key(aaaa1),
    key (`aaaa1`),
    key (`aaaa2`),
    key (`aaaa3`)    
) engine = akibadb;
 
create table aaaaa(
    aaaa1 int not null,
    aaaaa1 int not null,
    aaaaa2 int not null,
    aaaaa3 int not null,
    primary key(aaaaa1)
) engine = akibadb;
 
create table aaaab(
    aaaa1 int not null,
    aaaab1 int not null,
    aaaab2 int not null,
    aaaab3 int not null,
    primary key(aaaab1)
) engine = akibadb;

create table aaab(
    aaa1 int not null,
    aaab1 int not null,
    aaab2 int not null,
    aaab3 int not null,
    primary key(aaab1),
    key (`aaab1`, `aaab2`),
    key (`aaab2`, `aaab3`),
    key (`aaab3`, `aaab1`),
    key (`aaab1`, `aaab3`),
    key (`aaab2`, `aaab1`)
) engine = akibadb;
 
create table aab(
    aa1 int not null,
    aab1 int not null,
    aab2 int not null,
    aab3 int not null,
    primary key(aab1)
) engine = akibadb;
 
create table aac(
    aa1 int not null,
    aac1 int not null,
    aac2 int not null,
    aac3 int not null,
    primary key(aac1)
) engine = akibadb;
 
create table ab(
    a1 int not null,
    ab1 int not null,
    ab2 int not null,
    ab3 int not null,
    primary key(ab1)
) engine = akibadb;
