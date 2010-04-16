/*
schema akiba_information_schema baseid=100000;

group groups {table groups};
group tables {table tables};
group columns {table columns};
group joins {table joins};
group join_columns {table join_columns};
group indexes {table indexes};
group index_columns {table index_columns};
group types {table types};

*/

create table groups(
    group_name varchar(64) not null,
    primary key(group_name)
) engine = akibadb;

create table tables(
    schema_name       varchar(64) not null,
    table_name        varchar(64) not null,
    table_type        varchar(8) not null,
    table_id          smallint,
    group_name        varchar(64),
    primary key(schema_name, table_name),
    foreign key(group_name) references groups
) engine = akibadb;

create table columns (
    schema_name         varchar(64) not null,
    table_name          varchar(64) not null,
    column_name         varchar(64) not null,
    position            int not null, -- 0-based
    type                varchar(64) not null,
    type_param_1        bigint,
    type_param_2        bigint,
    nullable            tinyint not null,
    initial_autoinc     bigint,
    group_schema_name   varchar(64),
    group_table_name    varchar(64),
    group_column_name   varchar(64),
    maximum_size		bigint,
    prefix_size			int,
    primary key(schema_name, table_name, column_name)
) engine = akibadb;

create table joins(
    join_name               varchar(800) not null,
    parent_schema_name      varchar(64) not null,
    parent_table_name       varchar(64) not null,
    child_schema_name       varchar(64) not null,
    child_table_name        varchar(64) not null,
    group_name              varchar(64),
    join_weight             int,
    primary key(join_name)
) engine = akibadb;

create table join_columns(
    join_name               varchar(800) not null,
    parent_schema_name      varchar(64) not null,
    parent_table_name       varchar(64) not null,
    parent_column_name      varchar(64) not null,
    child_schema_name       varchar(64) not null,
    child_table_name        varchar(64) not null,
    child_column_name       varchar(64) not null,
    primary key(join_name, parent_column_name, child_column_name)
) engine = akibadb;

create table indexes (
    schema_name      varchar(64) not null,
    table_name       varchar(64) not null,
    index_name       varchar(64) not null,
    index_id         int not null,
    table_constraint varchar(64) not null,
    is_unique        tinyint not null,
    primary key(schema_name, table_name, index_name)
) engine = akibadb;

create table index_columns (
    schema_name       varchar(64) not null,
    table_name        varchar(64) not null,
    index_name        varchar(64) not null,
    column_name       varchar(64) not null,
    ordinal_position int not null,
    is_ascending     tinyint not null,
    primary key(schema_name, table_name, index_name, column_name)
) engine = akibadb;

create table types(
    type_name           varchar(64) not null,
    parameters          int not null,
    fixed_size          tinyint not null,
    max_size_bytes      bigint not null,
    primary key(type_name)
) engine = akibadb;

