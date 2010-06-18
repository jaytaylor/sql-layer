insert into akiba_information_schema.groups values ('access');
insert into akiba_information_schema.groups values ('construct');
insert into akiba_information_schema.tables values ('akiba_objects', '_akiba_access', 'GROUP', 1001, 'access');
insert into akiba_information_schema.tables values ('akiba_objects', '_akiba_construct', 'GROUP', 1003, 'construct');
insert into akiba_information_schema.tables values ('toy_test', 'gdm_access', 'USER', 1002, 'access');
insert into akiba_information_schema.tables values ('toy_test', 'gdm_construct', 'USER', 1004, 'construct');
insert into akiba_information_schema.tables values ('toy_test', 'gdm_sentence', 'USER', 1005, 'construct');
insert into akiba_information_schema.columns values ('akiba_objects', '_akiba_access', 'gdm_access$ACCESS_ID', 0, 'int', null, null, 0, null, null, null, null, 4, 0);
insert into akiba_information_schema.columns values ('akiba_objects', '_akiba_access', 'gdm_access$CLIENT_IP', 1, 'int', null, null, 0, null, null, null, null, 4, 0);
insert into akiba_information_schema.columns values ('akiba_objects', '_akiba_access', 'gdm_access$SERVER_IP', 2, 'int', null, null, 0, null, null, null, null, 4, 0);
insert into akiba_information_schema.columns values ('akiba_objects', '_akiba_access', 'gdm_access$BOOTSTRAP', 3, 'int', null, null, 0, null, null, null, null, 4, 0);
insert into akiba_information_schema.columns values ('akiba_objects', '_akiba_access', 'gdm_access$WEIGHT', 4, 'int', null, null, 0, null, null, null, null, 4, 0);
insert into akiba_information_schema.columns values ('akiba_objects', '_akiba_construct', 'gdm_construct$CONSTRUCT_ID', 0, 'int', null, null, 0, null, null, null, null, 4, 0);
insert into akiba_information_schema.columns values ('akiba_objects', '_akiba_construct', 'gdm_construct$ORIGINAL_SQL', 1, 'int', null, null, 0, null, null, null, null, 4, 0);
insert into akiba_information_schema.columns values ('akiba_objects', '_akiba_construct', 'gdm_sentence$SENTENCE_ID', 2, 'int', null, null, 0, null, null, null, null, 4, 0);
insert into akiba_information_schema.columns values ('akiba_objects', '_akiba_construct', 'gdm_sentence$CONSTRUCT_ID', 3, 'int', null, null, 0, null, null, null, null, 4, 0);
insert into akiba_information_schema.columns values ('akiba_objects', '_akiba_construct', 'gdm_sentence$VERB', 4, 'int', null, null, 0, null, null, null, null, 4, 0);
insert into akiba_information_schema.columns values ('akiba_objects', '_akiba_construct', 'gdm_sentence$DEPTH', 5, 'int', null, null, 0, null, null, null, null, 4, 0);
insert into akiba_information_schema.columns values ('toy_test', 'gdm_access', 'ACCESS_ID', 0, 'int', null, null, 0, null, 'akiba_objects', '_akiba_access', 'gdm_access$ACCESS_ID', 4, 0);
insert into akiba_information_schema.columns values ('toy_test', 'gdm_access', 'CLIENT_IP', 1, 'int', null, null, 0, null, 'akiba_objects', '_akiba_access', 'gdm_access$CLIENT_IP', 4, 0);
insert into akiba_information_schema.columns values ('toy_test', 'gdm_access', 'SERVER_IP', 2, 'int', null, null, 0, null, 'akiba_objects', '_akiba_access', 'gdm_access$SERVER_IP', 4, 0);
insert into akiba_information_schema.columns values ('toy_test', 'gdm_access', 'BOOTSTRAP', 3, 'int', null, null, 0, null, 'akiba_objects', '_akiba_access', 'gdm_access$BOOTSTRAP', 4, 0);
insert into akiba_information_schema.columns values ('toy_test', 'gdm_access', 'WEIGHT', 4, 'int', null, null, 0, null, 'akiba_objects', '_akiba_access', 'gdm_access$WEIGHT', 4, 0);
insert into akiba_information_schema.columns values ('toy_test', 'gdm_construct', 'CONSTRUCT_ID', 0, 'int', null, null, 0, null, 'akiba_objects', '_akiba_construct', 'gdm_construct$CONSTRUCT_ID', 4, 0);
insert into akiba_information_schema.columns values ('toy_test', 'gdm_construct', 'ORIGINAL_SQL', 1, 'int', null, null, 0, null, 'akiba_objects', '_akiba_construct', 'gdm_construct$ORIGINAL_SQL', 4, 0);
insert into akiba_information_schema.columns values ('toy_test', 'gdm_sentence', 'SENTENCE_ID', 0, 'int', null, null, 0, null, 'akiba_objects', '_akiba_construct', 'gdm_sentence$SENTENCE_ID', 4, 0);
insert into akiba_information_schema.columns values ('toy_test', 'gdm_sentence', 'CONSTRUCT_ID', 1, 'int', null, null, 0, null, 'akiba_objects', '_akiba_construct', 'gdm_sentence$CONSTRUCT_ID', 4, 0);
insert into akiba_information_schema.columns values ('toy_test', 'gdm_sentence', 'VERB', 2, 'int', null, null, 0, null, 'akiba_objects', '_akiba_construct', 'gdm_sentence$VERB', 4, 0);
insert into akiba_information_schema.columns values ('toy_test', 'gdm_sentence', 'DEPTH', 3, 'int', null, null, 0, null, 'akiba_objects', '_akiba_construct', 'gdm_sentence$DEPTH', 4, 0);
insert into akiba_information_schema.joins values ('toy_test:gdm_construct$gdm_sentence', 'toy_test', 'gdm_construct', 'toy_test', 'gdm_sentence', 'construct', 0);
insert into akiba_information_schema.join_columns values ('toy_test:gdm_construct$gdm_sentence', 'toy_test', 'gdm_construct', 'CONSTRUCT_ID', 'toy_test', 'gdm_sentence', 'CONSTRUCT_ID');
insert into akiba_information_schema.indexes values ('toy_test', 'gdm_access', 'GDM_ACCESS_I1', 2, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('toy_test', 'gdm_access', 'GDM_ACCESS_I2', 3, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('toy_test', 'gdm_access', 'PRIMARY', 1, 'PRIMARY KEY', 1);
insert into akiba_information_schema.indexes values ('toy_test', 'gdm_construct', 'PRIMARY', 1, 'PRIMARY KEY', 1);
insert into akiba_information_schema.indexes values ('toy_test', 'gdm_sentence', 'GDM_SENTENCE_I1', 3, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('toy_test', 'gdm_sentence', 'PRIMARY', 2, 'PRIMARY KEY', 1);
insert into akiba_information_schema.indexes values ('akiba_objects', '_akiba_access', '_akiba_access$gdm_access_PK', 1, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('akiba_objects', '_akiba_access', 'gdm_access$GDM_ACCESS_I1', 2, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('akiba_objects', '_akiba_access', 'gdm_access$GDM_ACCESS_I2', 3, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('akiba_objects', '_akiba_construct', '_akiba_construct$gdm_construct_PK', 1, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('akiba_objects', '_akiba_construct', '_akiba_construct$gdm_sentence_PK', 2, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('akiba_objects', '_akiba_construct', 'gdm_sentence$GDM_SENTENCE_I1', 3, 'INDEX', 0);
insert into akiba_information_schema.index_columns values ('toy_test', 'gdm_access', 'GDM_ACCESS_I1', 'CLIENT_IP', 0, 1);
insert into akiba_information_schema.index_columns values ('toy_test', 'gdm_access', 'GDM_ACCESS_I2', 'SERVER_IP', 0, 1);
insert into akiba_information_schema.index_columns values ('toy_test', 'gdm_access', 'PRIMARY', 'ACCESS_ID', 0, 1);
insert into akiba_information_schema.index_columns values ('toy_test', 'gdm_construct', 'PRIMARY', 'CONSTRUCT_ID', 0, 1);
insert into akiba_information_schema.index_columns values ('toy_test', 'gdm_sentence', 'GDM_SENTENCE_I1', 'CONSTRUCT_ID', 0, 1);
insert into akiba_information_schema.index_columns values ('toy_test', 'gdm_sentence', 'PRIMARY', 'SENTENCE_ID', 0, 1);
insert into akiba_information_schema.index_columns values ('toy_test', 'gdm_sentence', 'PRIMARY', 'CONSTRUCT_ID', 1, 1);
insert into akiba_information_schema.index_columns values ('akiba_objects', '_akiba_access', '_akiba_access$gdm_access_PK', 'gdm_access$ACCESS_ID', 0, 1);
insert into akiba_information_schema.index_columns values ('akiba_objects', '_akiba_access', 'gdm_access$GDM_ACCESS_I1', 'gdm_access$CLIENT_IP', 0, 1);
insert into akiba_information_schema.index_columns values ('akiba_objects', '_akiba_access', 'gdm_access$GDM_ACCESS_I2', 'gdm_access$SERVER_IP', 0, 1);
insert into akiba_information_schema.index_columns values ('akiba_objects', '_akiba_construct', '_akiba_construct$gdm_construct_PK', 'gdm_construct$CONSTRUCT_ID', 0, 1);
insert into akiba_information_schema.index_columns values ('akiba_objects', '_akiba_construct', '_akiba_construct$gdm_sentence_PK', 'gdm_sentence$SENTENCE_ID', 0, 1);
insert into akiba_information_schema.index_columns values ('akiba_objects', '_akiba_construct', '_akiba_construct$gdm_sentence_PK', 'gdm_sentence$CONSTRUCT_ID', 1, 1);
insert into akiba_information_schema.index_columns values ('akiba_objects', '_akiba_construct', 'gdm_sentence$GDM_SENTENCE_I1', 'gdm_sentence$CONSTRUCT_ID', 0, 1);

create table `akiba_objects`.`_akiba_access`(
  `gdm_access$ACCESS_ID` int,
  `gdm_access$CLIENT_IP` int,
  `gdm_access$SERVER_IP` int,
  `gdm_access$BOOTSTRAP` int,
  `gdm_access$WEIGHT` int,
  INDEX  `_akiba_access$gdm_access_PK` (`gdm_access$ACCESS_ID`),
  INDEX  `gdm_access$GDM_ACCESS_I1` (`gdm_access$CLIENT_IP`),
  INDEX  `gdm_access$GDM_ACCESS_I2` (`gdm_access$SERVER_IP`)
) engine=akibadb;

create table `akiba_objects`.`_akiba_construct`(
  `gdm_construct$CONSTRUCT_ID` int,
  `gdm_construct$ORIGINAL_SQL` int,
  `gdm_sentence$SENTENCE_ID` int,
  `gdm_sentence$CONSTRUCT_ID` int,
  `gdm_sentence$VERB` int,
  `gdm_sentence$DEPTH` int,
  INDEX  `_akiba_construct$gdm_construct_PK` (`gdm_construct$CONSTRUCT_ID`),
  INDEX  `_akiba_construct$gdm_sentence_PK` (`gdm_sentence$SENTENCE_ID`, `gdm_sentence$CONSTRUCT_ID`),
  INDEX  `gdm_sentence$GDM_SENTENCE_I1` (`gdm_sentence$CONSTRUCT_ID`)
) engine=akibadb;
