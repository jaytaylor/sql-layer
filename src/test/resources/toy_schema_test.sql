-- akiba_information_schema database

-- source ../sql/akiba_information_schema.sql
-- use akiba_information_schema;

drop database if exists toy_test;
create database toy_test;

-- don't forget the grants to allow access
grant all on toy_test.* to akiba, akiba@'localhost';

use akiba_information_schema;

-- Tables

source toy_schema.sql 

-- USER TABLES

use toy_test;

source toy_schema.ddl
-- source toy_schema_data.sql
