#!/bin/bash


pushd /usr/local/akiba
# service mysql stop
sudo killall -9 mysqld mysqld_safe
sudo rm -rf var
sleep 2

netstat -an | grep 3306
echo If there are no TIMED_WAIT connections, press ENTER to continue
read $fred

sudo bin/mysql_install_db -u root
sudo chown -R mysql.mysql var
sudo service mysql start
sleep 2

bin/mysql -u root <<EOF
create user 'akiba'@'localhost' identified by 'akibaDB';
install plugin akibadb soname 'libakibadb_engine.so';
EOF
popd

sleep 3

echo Loading data dictionary

mysql -u root <<EOF
drop database if exists akiba_objects;
drop database if exists akiba_information_schema;

-- create user akiba identified by 'akibaDB';
-- create user akiba@'localhost' identified by 'akibaDB';

-- The akiba_information_schema holds the metadata for the akiba tables
-- and engine.
create database akiba_information_schema;

-- The akiba_objects database is, by convention not requirement,
-- used to hold the akibaDB internal (Group) tables, and other objects.
-- This allows us to keep the akiba objects safe from accidental
-- manipulation by the end user.
create database akiba_objects;

grant all on akiba_information_schema.* to akiba, akiba@'localhost';
grant all on akiba_objects.* to akiba, akiba@'localhost';

use akiba_information_schema;

-- The additional table information for the Akiba_information_schema.
-- The base information for tables is in INFORMATION_SCHEMA.TABLES
-- The two types of tables are group tables (created as part of the grouping
-- process internal to akibaDB) and user tables (created by the user as
-- their needs dictated).

create table tables(
    schema_name       varchar(64) not null,
    table_name        varchar(64) not null,
    table_type        enum ('USER', 'GROUP') not null,
    -- table_id describes ordering of user tables in the group.
    -- table_id is 0 for group tables or single user tables, 
    table_id		  smallint,
    -- parent_schema, parent_name and join_weight describe the group structure:
    -- * USER table: references parent table in group. For a user table
    --               that is the root of its group, the parent* fields identify
    --               the group table, and weight is null.
    -- * GROUP table: null
    parent_schema      varchar(64),
    parent_name        varchar(64),
    join_weight        int,
    primary key (schema_name, table_name),
    foreign key (parent_schema, parent_name) references tables(schema_name, table_name)
) engine = myisam;

-- Column information, combining information copied from the mysql
-- information schema and akiba information.
-- group_* columns: In columns of USER tables, the group_* columns refer to columns
--     in GROUP tables. In GROUP tables, these columns are null.
-- parent_* columns: In columns of USER tables, these columns specify column-level
--     join information, for the joins that define groups.
create table columns (
    schema_name      varchar(64) not null,
    table_name       varchar(64) not null,
    column_name      varchar(64) not null,
    position         int not null, -- 0-based
    type             varchar(64) not null, -- enum ('INT', 'TINYINT', 'VARCHAR') not null,
    type_param_1     bigint, -- null if not needed for the type
    type_param_2     bigint, -- null if not needed for the type
    nullable         tinyint not null, -- 1 = true, 0 = false
    group_schema_name   varchar(64),
    group_table_name    varchar(64),
    group_column_name   varchar(64),
    parent_schema_name  varchar(64),
    parent_table_name   varchar(64),
    parent_column_name  varchar(64),
    primary key (schema_name, table_name, column_name),
    foreign key (schema_name, table_name) references tables(schema_name, table_name),
    foreign key (group_schema_name, group_table_name, group_column_name)
        references columns(schema_name, table_name, column_name),
    foreign key (parent_schema_name, parent_table_name, parent_column_name)
        references columns(schema_name, table_name, column_name)
) engine = myisam;

-- Index information for each table. MySQL Information schema has their key information kept in two tables:
-- INFORMATION_SCHEMA.TABLE_CONSTRAINTS and REFERENTIAL_CONSTRAINTS. The table_constraints match the MySQL key
-- information. 
create table indexes (
	schema_name      varchar(64) not null,
	table_name       varchar(64) not null,
	index_name       varchar(64) not null,
	table_constraint varchar(64) not null   COMMENT 'one of PRIMARY KEY, FOREIGN KEY, UNIQUE, or INDEX',
	is_unique        tinyint not null       COMMENT '1 = true, 0 = false',
	primary key (schema_name, table_name, index_name),
	foreign key (schema_name, table_name) references tables(schema_name, table_name)
) engine = myisam;

-- List of the columns within each index. The MySQL Information Schema keeps their copy of the information
-- in the KEY_COLUMN_USAGE table. 
create table index_columns (
	schema_name       varchar(64) not null,
	table_name        varchar(64) not null,
	index_name        varchar(64) not null,
	column_name       varchar(64) not null,
	ordinal_position int not null default -1  COMMENT 'position within the index, 0 based',
	is_ascending     tinyint not null         COMMENT '1 = true, 0 = false',
	primary key (schema_name, table_name, index_name, column_name),
	foreign key (schema_name, table_name, index_name)
            references indexes(schema_name, table_name, index_name),
	foreign key (schema_name, table_name, column_name)
            references columns(schema_name, table_name, column_name)
) engine = myisam;

-- The group table holds information regarding groups within
-- the akiba_objects schema. A group is a collection of one or more tables,
-- all connected by a set of joins.
-- The chunk server stores groups, even if the group contains only one table.
-- All user tables belong to a group, even if it is a group with the single
-- table.
create table groups(
    group_name          varchar(64) not null,
    group_table_schema  varchar(64) not null,
    group_table_name    varchar(64) not null,
    primary key (group_name),
    foreign key (group_table_schema, group_table_name) references tables(schema_name, table_name)
) engine = myisam;



-- akiba_information_schema database

use akiba_information_schema;

drop database if exists data_dictionary_test;
create database data_dictionary_test;

-- don't forget the grants to allow access
grant all on data_dictionary_test.* to akiba, akiba@'localhost';

-- -----------------------------------------------------------------------------

use akiba_information_schema;

-- Tables

-- -- Group
insert into tables values('akiba_objects', 'coi', 'GROUP', 0, null, null, null);

-- -- User
insert into tables values('data_dictionary_test', 'customer', 'USER', 1,  'akiba_objects', 'coi', null);
insert into tables values('data_dictionary_test', 'order', 'USER', 2, 'data_dictionary_test', 'customer', 0);
insert into tables values('data_dictionary_test', 'item', 'USER', 3, 'data_dictionary_test', 'order', 0);


-- Columns

-- -- Group

insert into columns values('akiba_objects', 'coi', 'customer\$customer_id', 0,
                            'INT', null, null, 0,
                             null, null, null,
                             null, null, null);
insert into columns values('akiba_objects', 'coi', 'customer\$customer_name', 1,
                            'VARCHAR', 100, null, 0,
                            null, null, null,
                            null, null, null);
insert into columns values('akiba_objects', 'coi', 'order\$order_id', 2,
                            'INT', null, null, 0,
                            null, null, null,
                            null, null, null);
insert into columns values('akiba_objects', 'coi', 'order\$customer_id', 3,
                            'INT', null, null, 0,
                            null, null, null,
                            null, null, null);
insert into columns values('akiba_objects', 'coi', 'order\$order_date', 4,
                            'INT', null, null, 0,
                            null, null, null,
                            null, null, null);
insert into columns values('akiba_objects', 'coi', 'item\$order_id', 5,
                            'INT', null, null, 0,
                            null, null, null,
                            null, null, null);
insert into columns values('akiba_objects', 'coi', 'item\$part_id', 6,
                            'INT', null, null, 0,
                            null, null, null,
                            null, null, null);
insert into columns values('akiba_objects', 'coi', 'item\$quantity', 7,
                            'INT', null, null, 0,
                            null, null, null,
                            null, null, null);
insert into columns values('akiba_objects', 'coi', 'item\$unit_price', 8,
                            'INT', null, null, 0,
                            null, null, null,
                            null, null, null);

-- -- User

insert into columns values('data_dictionary_test', 'customer', 'customer_id', 0,
                            'INT', null, null, 0,
                            'akiba_objects', 'coi', 'customer\$customer_id',
                            null, null, null);
insert into columns values('data_dictionary_test', 'customer', 'customer_name', 1,
                            'VARCHAR', 100, null, 0,
                            'akiba_objects', 'coi', 'customer\$customer_name',
                            null, null, null);
insert into columns values('data_dictionary_test', 'order', 'order_id', 0,
                            'INT', null, null, 0,
                            'akiba_objects', 'coi', 'order\$order_id',
                            null, null, null);
insert into columns values('data_dictionary_test', 'order', 'customer_id', 1,
                            'INT', null, null, 0,
                            'akiba_objects', 'coi', 'order\$customer_id',
                            'data_dictionary_test', 'customer', 'customer_id');
insert into columns values('data_dictionary_test', 'order', 'order_date', 2,
                            'INT', null, null, 0,
                            'akiba_objects', 'coi', 'order\$order_date',
                            null, null, null);
insert into columns values('data_dictionary_test', 'item', 'order_id', 0,
                            'INT', null, null, 0,
                            'akiba_objects', 'coi', 'item\$order_id',
                            'data_dictionary_test', 'order', 'order_id');
insert into columns values('data_dictionary_test', 'item', 'part_id', 1,
                            'INT', null, null, 0,
                            'akiba_objects', 'coi', 'item\$part_id',
                            null, null, null);
insert into columns values('data_dictionary_test', 'item', 'quantity', 2,
                            'INT', null, null, 0,
                            'akiba_objects', 'coi', 'item\$quantity',
                            null, null, null);
insert into columns values('data_dictionary_test', 'item', 'unit_price', 3,
                            'INT', null, null, 0,
                            'akiba_objects', 'coi', 'item\$unit_price',
                            null, null, null);

-- Indexes 
insert into indexes values ('akiba_objects', 'coi', 'coi_customer_PK', 'PRIMARY KEY', 1);
insert into index_columns values ('akiba_objects', 'coi', 'coi_customer_PK', 'customer\$customer_id', 0, 1); 

insert into indexes values ('akiba_objects', 'coi', 'coi_order_PK', 'PRIMARY KEY', 1);
insert into index_columns values ('akiba_objects', 'coi', 'coi_order_PK', 'order\$order_id', 0, 1);

insert into indexes values ('akiba_objects', 'coi', 'coi_item_PK', 'PRIMARY KEY', 1);
insert into index_columns  values ('akiba_objects', 'coi', 'coi_item_PK', 'order\$order_id', 0, 1);
insert into index_columns  values ('akiba_objects', 'coi', 'coi_item_PK', 'item\$part_id', 1, 1);

insert into indexes values ('data_dictionary_test', 'customer', 'customer_PK', 'PRIMARY KEY', 1);
insert into index_columns values ('data_dictionary_test', 'customer', 'customer_PK', 'customer_id', 0, 1); 

insert into indexes values ('data_dictionary_test', 'order', 'order_PK', 'PRIMARY KEY', 1);
insert into index_columns values ('data_dictionary_test', 'order', 'order_PK', 'order_id', 0, 1);

insert into indexes values ('data_dictionary_test', 'item', 'item_PK', 'PRIMARY KEY', 1);
insert into index_columns values ('data_dictionary_test', 'item', 'item_PK', 'order_id', 0, 1);
insert into index_columns values ('data_dictionary_test', 'item', 'item_PK', 'part_id', 1, 1);

 
-- Groups

insert into groups values('coi', 'akiba_objects', 'coi');

-- -----------------------------------------------------------------------------

-- USER TABLES

use data_dictionary_test;

create table customer(
    customer_id int not null,
    customer_name varchar(100) not null,
    primary key(customer_id)
) engine = akibadb;

create table \`order\`(
    order_id int not null,
    customer_id int not null,
    order_date int not null,
    primary key(order_id),
    foreign key(customer_id) references customer
) engine = akibadb;

create table item(
    order_id int not null,
    part_id int not null,
    quantity int not null,
    unit_price int not null,
    primary key(order_id, part_id),
    foreign key(order_id) references \`order\`
) engine = akibadb;

-- GROUP TABLES

use akiba_objects;

create table coi(
    customer\$customer_id int not null,
    customer\$customer_name varchar(100) not null,
    order\$order_id int not null,
    order\$order_date int not null,
    item\$part_id int not null,
    item\$quantity int not null,
    item\$unit_price int not null
) engine = akibadb;
EOF


