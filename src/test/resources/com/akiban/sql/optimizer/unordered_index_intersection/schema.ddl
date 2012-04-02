create table t(
    id int not null,
    orderby int,
    a int,
    b int,
    c int,
    filler varchar(500),
    primary key(id)
);

create index idx_a_orderby on t(a, orderby);
create index idx_b on t(b);
create index idx_c on t(c);
