create table akiban_information_schema.groups (
    group_name varchar(64) not null,
    primary key(group_name)
) engine=akibandb;

create table akiban_information_schema.tables (
    schema_name       varchar(64) not null,
    table_name        varchar(64) not null,
    table_type        varchar(8),
    table_id          int,
    group_name        varchar(64),
    source_types      int,
    tree_name         varchar(160) not null, -- max schema + table name length (and slack)
    primary key(schema_name, table_name)
) engine=akibandb;

create table akiban_information_schema.columns (
    schema_name         varchar(64) not null,
    table_name          varchar(64) not null,
    column_name         varchar(64) not null,
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
    character_set       varchar(32),
    collation           varchar(32),
    primary key(schema_name, table_name, column_name)
) engine=akibandb;

create table akiban_information_schema.joins (
    join_name               varchar(255) not null,
    parent_schema_name      varchar(64),
    parent_table_name       varchar(64),
    child_schema_name       varchar(64),
    child_table_name        varchar(64),
    group_name              varchar(64),
    join_weight             int,
    grouping_usage          int,
    source_types            int,
    primary key(join_name)
) engine=akibandb;

create table akiban_information_schema.join_columns (
    join_name               varchar(255) not null,
    parent_schema_name      varchar(64),
    parent_table_name       varchar(64),
    parent_column_name      varchar(64) not null,
    child_schema_name       varchar(64),
    child_table_name        varchar(64),
    child_column_name       varchar(64) not null,
    primary key(join_name, parent_column_name, child_column_name)
) engine=akibandb;

create table akiban_information_schema.indexes (
    schema_name      varchar(64) not null,
    table_name       varchar(64) not null,
    index_name       varchar(64) not null,
    index_type       varchar(64),
    index_id         int,
    table_constraint varchar(64),
    is_unique        tinyint,
    tree_name        varchar(224) not null, -- max schema + table + index name length (and slack)
    primary key(schema_name, table_name, index_name)
) engine=akibandb;

create table akiban_information_schema.index_columns (
    schema_name       varchar(64) not null,
    table_name        varchar(64) not null,
    index_name        varchar(64) not null,
    index_type        varchar(64),
    column_name       varchar(64) not null,
    ordinal_position  int,
    is_ascending      tinyint,
    indexed_length    int,
    primary key(schema_name, table_name, index_name, column_name)
) engine=akibandb;

create table akiban_information_schema.types (
    type_name           varchar(64) not null,
    parameters          int,
    fixed_size          tinyint,
    max_size_bytes      bigint,
    primary key(type_name)
) engine=akibandb;

create table akiban_information_schema.index_analysis (
    table_id            int not null,
    index_id            int not null,
    analysis_timestamp  timestamp,
    item_number         int not null,
    key_string          varchar(2048),
    index_row_data      varbinary(4096),
    count               bigint,
    primary key(table_id, index_id, item_number)
) engine=akibandb;

-- New tables must be alphabetically after old ones to maintain database compatibility.

create table akiban_information_schema.zindex_statistics (
    table_id            int not null,
    index_id            int not null,
    analysis_timestamp  timestamp,
    row_count           bigint,
    sampled_count       bigint,
    primary key(table_id, index_id)
) engine=akibandb;

create table akiban_information_schema.zindex_statistics_entry (
    table_id            int not null,
    index_id            int not null,
    column_count        int not null,
    item_number         int not null,
    key_string          varchar(2048),
    key_bytes           varbinary(4096),
    eq_count            bigint,
    lt_count            bigint,
    distinct_count      bigint,
    primary key(table_id, index_id, column_count, item_number),
    CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (table_id, index_id) REFERENCES akiban_information_schema.zindex_statistics(table_id, index_id)
) engine=akibandb;

