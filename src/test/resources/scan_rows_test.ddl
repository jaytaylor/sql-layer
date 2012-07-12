create table a(
    a0 int not null,
    a1 int not null,
    a2 int not null,
    a3 int not null,
    a4 varchar(64) not null collate latin1_swedish_ci,
    primary key(a1)
);

create table aa(
    a1 int not null,
    aa1 int not null,
    aa2 int not null,
    aa3 int not null,
    aa4 varchar(64) not null collate latin1_swedish_ci,
    primary key(aa1),
    GROUPING FOREIGN KEY (a1) REFERENCES a (a1)
);
create index aa2 on aa(aa2, aa3);
create index aa22 on aa(aa2, aa1);
create index str on aa(aa4);

create table aaa(
    aa1 int not null,
    aaa1 int not null,
    aaa2 int not null,
    aaa3 int not null,
    aaa4 varchar(64) not null collate latin1_swedish_ci,
    primary key(aaa1),
    GROUPING FOREIGN KEY (aa1) REFERENCES aa (aa1)
);
create index aaa1 on aaa(aaa1);
create index aaa2 on aaa(aaa2);
create index aaa3 on aaa(aaa3);

create table aaaa(
    aaa1 int not null,
    aaaa1 int not null,
    aaaa2 int not null,
    aaaa3 int not null,
    aaaa4 varchar(64) not null collate latin1_swedish_ci,
    primary key(aaaa1),
    GROUPING FOREIGN KEY (aaa1) REFERENCES aaa (aaa1)
);
create index aaaa1 on aaaa(aaaa1);
create index aaaa2 on aaaa(aaaa2);
create index aaaa3 on aaaa(aaaa3);

create table aaaaa(
    aaaa1 int not null,
    aaaaa1 int not null,
    aaaaa2 int not null,
    aaaaa3 int not null,
    aaaaa4 varchar(64) not null collate latin1_swedish_ci,
    primary key(aaaaa1),
    GROUPING FOREIGN KEY (aaaa1) REFERENCES aaaa (aaaa1)
);

create table aaaab(
    aaaa1 int not null,
    aaaab1 int not null,
    aaaab2 int not null,
    aaaab3 int not null,
    aaaab4 varchar(64) not null collate latin1_swedish_ci,
    primary key(aaaab1),
    GROUPING FOREIGN KEY (aaaa1) REFERENCES aaaa (aaaa1)
);

create table aaab(
    aaa1 int not null,
    aaab1 int not null,
    aaab2 int not null,
    aaab3 int not null,
    aaab4 varchar(64) not null collate latin1_swedish_ci,
    primary key(aaab1),
    GROUPING FOREIGN KEY (aaa1) REFERENCES aaa (aaa1)
);
create index aaab1 on aaab(aaab1, aaab2);
create index aaab2 on aaab(aaab2, aaab3);
create index aaab3aaab1 on aaab(aaab3, aaab1);
create index aaab12 on aaab(aaab1, aaab3);
create index aaab22 on aaab(aaab2, aaab1);

create table aab(
    aa1 int not null,
    aab1 int not null,
    aab2 int not null,
    aab3 int not null,
    aab4 varchar(64) not null collate latin1_swedish_ci,
    primary key(aab1),
    GROUPING FOREIGN KEY (aa1) REFERENCES aa (aa1)
);

create table aac(
    aa1 int not null,
    aac1 int not null,
    aac2 int not null,
    aac3 int not null,
    aac4 varchar(64) not null collate latin1_swedish_ci,
    primary key(aac1),
    GROUPING FOREIGN KEY (aa1) REFERENCES aa (aa1)
);

create table ab(
    a1 int not null,
    ab1 int not null,
    ab2 int not null,
    ab3 int not null,
    ab4 varchar(64) not null collate latin1_swedish_ci,
    primary key(ab1),
    GROUPING FOREIGN KEY (a1) REFERENCES a (a1)
);

create table bug253(
    vid int not null,
    type varchar(255) not null collate latin1_swedish_ci,
    primary key (type, vid)
);
create index vid on bug253(vid);

