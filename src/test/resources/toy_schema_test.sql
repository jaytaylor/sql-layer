
drop database if exists toy_test;
create database toy_test;

grant all on toy_test.* to akiba, akiba@'localhost';

use akiba_information_schema;

source toy_schema.sql 

use toy_test;
source toy_schema.ddl
