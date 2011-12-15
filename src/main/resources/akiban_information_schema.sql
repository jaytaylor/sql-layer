--
-- Copyright (C) 2011 Akiban Technologies Inc.
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License, version 3,
-- as published by the Free Software Foundation.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU Affero General Public License for more details.
--
-- You should have received a copy of the GNU Affero General Public License
-- along with this program.  If not, see http://www.gnu.org/licenses.
--

drop database if exists akiban_information_schema;

create database akiban_information_schema;

-- These grants ensure a user exists if it doesn't already. This is done so that drop user succeeds, so that
-- create user succeeds. All this is necessary to make sure that a user exists whether or not it
-- existed before this script was run.
grant usage on *.* to akiban;
grant usage on *.* to akiban@'localhost';
drop user akiban;
drop user akiban@'localhost';
create user akiban identified by 'akibanDB';
create user akiban@'localhost' identified by 'akibanDB';
grant all on akiban_information_schema.* to akiban, akiban@'localhost';

use akiban_information_schema;

create table groups(
    group_name        varchar(64) not null,
    primary key(group_name)
) engine=akibandb;


-- Kinds of tables:
--    * table_type = USER, group_name = NULL: Discovered user table, not in any group.
--    * table_type = USER, group_name != NULL: User table in a group.
--    * table_type = GROUP, group_name != NULL: Group table.
-- migration_usage values: AKIBAN_STANDARD = 0, AKIBAN_LOOKUP_TABLE = 1, KEEP_ENGINE = 2, INCOMPATIBLE = 3
create table tables(
    schema_name       varchar(64) not null,
    table_name        varchar(64) not null,
    table_type        varchar(8) not null,
    table_id          int,
    group_name        varchar(64),
    migration_usage   int not null,
    tree_name         varchar(160) not null,
    primary key(schema_name, table_name),
    foreign key(group_name) references groups
) engine=akibandb;

-- Column information, combining information copied from the mysql
-- information schema and akiban information.
-- group_* columns: In columns of USER tables, the group_* columns refer to columns
--     in GROUP tables. In GROUP tables, these columns are null.
-- parent_* columns: In columns of USER tables, these columns specify column-level
--     join information, for the joins that define groups.
--
--
--  position          -- 0-based
--  type              -- enum ('INT', 'TINYINT', 'VARCHAR') not null,
--  type_param_1      -- null if not needed for the type
--  type_param_2      -- null if not needed for the type
--  nullable          -- 1 = true, 0 = false

create table columns (
    schema_name         varchar(64) not null,
    table_name          varchar(64) not null,
    column_name         varchar(64) not null,
    position            int not null, 
    type                varchar(64) not null, 
    type_param_1        bigint, 
    type_param_2        bigint, 
    nullable            tinyint not null, 
    initial_autoinc     bigint,
    group_schema_name   varchar(64),
    group_table_name    varchar(64),
    group_column_name   varchar(64),
    max_storage_size    bigint not null,
    prefix_size         int not null,
    character_set       varchar(32) not null,
    collation           varchar(32) not null,
    primary key(schema_name, table_name, column_name),
    foreign key(schema_name, table_name) references tables(schema_name, table_name),
    foreign key(group_schema_name, group_table_name, group_column_name)
        references columns(schema_name, table_name, column_name)
) engine=akibandb;


-- join_name           --  for now size large until name convention is determined
-- grouping_usage values: ALWAYS = 0, NEVER = 1, WHEN_OPTIMAL = 2, IGNORE = 3
-- source_types values: FK = 1 << 0, COLUMN_NAME = 1 << 1, QUERY = 1 << 2, USER = 1 << 3
create table joins(
    join_name               varchar(512) not null, 
    parent_schema_name      varchar(64) not null,
    parent_table_name       varchar(64) not null,
    child_schema_name       varchar(64) not null,
    child_table_name        varchar(64) not null,
    group_name              varchar(64),
    join_weight             int,
    grouping_usage          int not null,
    source_types            int not null,
    primary key(join_name),
    foreign key(group_name) references groups(group_name),
    foreign key(parent_schema_name, parent_table_name) references tables(schema_name, table_name),
    foreign key(child_schema_name, child_table_name) references tables(schema_name, table_name)
) engine=akibandb;

-- join_name: for now size large until name convention is determined
create table join_columns(
    join_name               varchar(512) not null, 
    parent_schema_name      varchar(64) not null,
    parent_table_name       varchar(64) not null,
    parent_column_name      varchar(64) not null,
    child_schema_name       varchar(64) not null,
    child_table_name        varchar(64) not null,
    child_column_name       varchar(64) not null,
    primary key(join_name, parent_column_name, child_column_name),
    foreign key(join_name) references joins(join_name),
    foreign key(parent_schema_name, parent_table_name, parent_column_name)
        references columns(schema_name, table_name, column_name),
    foreign key(child_schema_name, child_table_name, child_column_name)
        references columns(schema_name, table_name, column_name)
) engine=akibandb;

-- Index information for each table. MySQL Information schema has their key information kept in two tables:
-- INFORMATION_SCHEMA.TABLE_CONSTRAINTS and REFERENTIAL_CONSTRAINTS. The table_constraints match the MySQL key
-- information. 
create table indexes (
    schema_name      varchar(64) not null,
    table_name       varchar(64) not null,
    index_name       varchar(64) not null,
    index_type       varchar(64) not null,
    index_id         int not null,
    table_constraint varchar(64) not null   COMMENT 'one of PRIMARY KEY, FOREIGN KEY, UNIQUE, or INDEX',
    is_unique        tinyint not null       COMMENT '1 = true, 0 = false',
    tree_name        varchar(224) not null,
    primary key(schema_name, table_name, index_name),
    foreign key(schema_name, table_name) references tables(schema_name, table_name)
) engine=akibandb;

-- List of the columns within each index. The MySQL Information Schema keeps their copy of the information
-- in the KEY_COLUMN_USAGE table. 
create table index_columns (
    schema_name       varchar(64) not null,
    table_name        varchar(64) not null,
    index_name        varchar(64) not null,
    index_type       varchar(64) not null,
    column_name       varchar(64) not null,
    ordinal_position int not null default -1  COMMENT 'position within the index, 0 based',
    is_ascending     tinyint not null         COMMENT '1 = true, 0 = false',
    indexed_length   int,
    primary key(schema_name, table_name, index_name, column_name),
    foreign key(schema_name, table_name, index_name)
            references indexes(schema_name, table_name, index_name),
    foreign key(schema_name, table_name, column_name)
            references columns(schema_name, table_name, column_name)
) engine=akibandb;

-- Information on mysql types.
--  fixed_size          -- 1: fixed, 0: variable
create table types(
    type_name           varchar(64) not null,
    parameters          int not null,
    fixed_size          tinyint not null, 
    max_size_bytes      bigint not null
) engine=akibandb;

-- Index Histogram data computed by AkSserver when requested by
-- ANALYZE TABLE. Column key_string is for human consumption
-- only.  Column index_row_data contains a RowData-formatted
-- byte array to be sent as part of the histogram.
create table index_analysis(
    table_id            int,
    index_id            int,
    analysis_timestamp  timestamp,
    item_number         int,
    key_string          varchar(2048),
    index_row_data      varbinary(4096),
    count               bigint,
    primary key(table_id, index_id, item_number)
) engine=akibandb;

-- New format Index histograms
-- See akiban_information_schema.ddl for note on names.
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
    index_row_data      varbinary(4096),
    eq_count            bigint,
    lt_count            bigint,
    distinct_count      bigint,
    primary key(table_id, index_id, column_count, item_number),
    CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (table_id, index_id) REFERENCES akiban_information_schema.zindex_statistics(table_id, index_id)
) engine=akibandb;

