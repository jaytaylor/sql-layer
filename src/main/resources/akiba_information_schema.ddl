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
group index_analysis {table index_analysis}; 
*/

create table groups(
    group_name varchar(64),
    primary key(group_name)
) engine = akibadb;

create table tables(
    schema_name       varchar(64),
    table_name        varchar(64),
    table_type        varchar(8),
    table_id          smallint,
    group_name        varchar(64),
    primary key(schema_name, table_name),
    foreign key(group_name) references groups
) engine = akibadb;

create table columns (
    schema_name         varchar(64),
    table_name          varchar(64),
    column_name         varchar(64),
    position            int, -- 0-based
    type                varchar(64),
    type_param_1        bigint,
    type_param_2        bigint,
    nullable            tinyint,
    initial_autoinc     bigint,
    group_schema_name   varchar(64),
    group_table_name    varchar(64),
    group_column_name   varchar(64),
    maximum_size		bigint,
    prefix_size			int,
    primary key(schema_name, table_name, column_name)
) engine = akibadb;

create table joins(
    join_name               varchar(800),
    parent_schema_name      varchar(64),
    parent_table_name       varchar(64),
    child_schema_name       varchar(64),
    child_table_name        varchar(64),
    group_name              varchar(64),
    join_weight             int,
    primary key(join_name)
) engine = akibadb;

create table join_columns(
    join_name               varchar(800),
    parent_schema_name      varchar(64),
    parent_table_name       varchar(64),
    parent_column_name      varchar(64),
    child_schema_name       varchar(64),
    child_table_name        varchar(64),
    child_column_name       varchar(64),
    primary key(join_name, parent_column_name, child_column_name)
) engine = akibadb;

create table indexes (
    schema_name      varchar(64),
    table_name       varchar(64),
    index_name       varchar(64),
    index_id         int,
    table_constraint varchar(64),
    is_unique        tinyint,
    primary key(schema_name, table_name, index_name)
) engine = akibadb;

create table index_columns (
    schema_name       varchar(64),
    table_name        varchar(64),
    index_name        varchar(64),
    column_name       varchar(64),
    ordinal_position int,
    is_ascending     tinyint,
    primary key(schema_name, table_name, index_name, column_name)
) engine = akibadb;

create table types(
    type_name           varchar(64),
    parameters          int,
    fixed_size          tinyint,
    max_size_bytes      bigint,
    primary key(type_name)
) engine = akibadb;

create table index_analysis(
    table_id            int,
    index_id            int,
    analysis_timestamp  timestamp,
    item_number         int,
    key_string          varchar(2048),
    index_row_data      varbinary(65535),
    count               bigint,
    primary key(table_id, index_id, item_number)
) engine = akibadb;


