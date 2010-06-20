insert into akiba_information_schema.groups values ('coi');
insert into akiba_information_schema.tables values ('akiba_objects', '_akiba_coi', 'GROUP', 1, 'coi');
insert into akiba_information_schema.tables values ('toy_test', 'customers', 'USER', 2, 'coi');
insert into akiba_information_schema.tables values ('toy_test', 'items', 'USER', 3, 'coi');
insert into akiba_information_schema.tables values ('toy_test', 'orders', 'USER', 4, 'coi');
insert into akiba_information_schema.columns values ('akiba_objects', '_akiba_coi', 'customers$customer_id', 0, 'int', null, null, 0, null, null, null, null, 4, 0);
insert into akiba_information_schema.columns values ('akiba_objects', '_akiba_coi', 'orders$order_id', 1, 'int', null, null, 0, null, null, null, null, 4, 0);
insert into akiba_information_schema.columns values ('akiba_objects', '_akiba_coi', 'orders$customer_id', 2, 'int', null, null, 1, null, null, null, null, 4, 0);
insert into akiba_information_schema.columns values ('akiba_objects', '_akiba_coi', 'items$item_id', 3, 'int', null, null, 0, null, null, null, null, 4, 0);
insert into akiba_information_schema.columns values ('akiba_objects', '_akiba_coi', 'items$order_id', 4, 'int', null, null, 1, null, null, null, null, 4, 0);
insert into akiba_information_schema.columns values ('toy_test', 'customers', 'customer_id', 0, 'int', null, null, 0, null, 'akiba_objects', '_akiba_coi', 'customers$customer_id', 4, 0);
insert into akiba_information_schema.columns values ('toy_test', 'items', 'item_id', 0, 'int', null, null, 0, null, 'akiba_objects', '_akiba_coi', 'items$item_id', 4, 0);
insert into akiba_information_schema.columns values ('toy_test', 'items', 'order_id', 1, 'int', null, null, 1, null, 'akiba_objects', '_akiba_coi', 'items$order_id', 4, 0);
insert into akiba_information_schema.columns values ('toy_test', 'orders', 'order_id', 0, 'int', null, null, 0, null, 'akiba_objects', '_akiba_coi', 'orders$order_id', 4, 0);
insert into akiba_information_schema.columns values ('toy_test', 'orders', 'customer_id', 1, 'int', null, null, 1, null, 'akiba_objects', '_akiba_coi', 'orders$customer_id', 4, 0);
insert into akiba_information_schema.joins values ('toy_test:customers$orders', 'toy_test', 'customers', 'toy_test', 'orders', 'coi', 0);
insert into akiba_information_schema.joins values ('toy_test:orders$items', 'toy_test', 'orders', 'toy_test', 'items', 'coi', 0);
insert into akiba_information_schema.join_columns values ('toy_test:customers$orders', 'toy_test', 'customers', 'customer_id', 'toy_test', 'orders', 'customer_id');
insert into akiba_information_schema.join_columns values ('toy_test:orders$items', 'toy_test', 'orders', 'order_id', 'toy_test', 'items', 'order_id');
insert into akiba_information_schema.indexes values ('toy_test', 'customers', 'customer_id', 2, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('toy_test', 'customers', 'PRIMARY', 1, 'PRIMARY KEY', 1);
insert into akiba_information_schema.indexes values ('toy_test', 'items', 'item_id', 4, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('toy_test', 'items', 'order_id', 5, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('toy_test', 'items', 'PRIMARY', 3, 'PRIMARY KEY', 1);
insert into akiba_information_schema.indexes values ('toy_test', 'orders', 'customer_id', 8, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('toy_test', 'orders', 'order_id', 7, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('toy_test', 'orders', 'PRIMARY', 6, 'PRIMARY KEY', 1);
insert into akiba_information_schema.indexes values ('akiba_objects', '_akiba_coi', '_akiba_coi$customers_PK', 1, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('akiba_objects', '_akiba_coi', '_akiba_coi$items_PK', 3, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('akiba_objects', '_akiba_coi', '_akiba_coi$orders_PK', 6, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('akiba_objects', '_akiba_coi', 'customers$customer_id', 2, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('akiba_objects', '_akiba_coi', 'items$item_id', 4, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('akiba_objects', '_akiba_coi', 'items$order_id', 5, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('akiba_objects', '_akiba_coi', 'orders$customer_id', 8, 'INDEX', 0);
insert into akiba_information_schema.indexes values ('akiba_objects', '_akiba_coi', 'orders$order_id', 7, 'INDEX', 0);
insert into akiba_information_schema.index_columns values ('toy_test', 'customers', 'customer_id', 'customer_id', 0, 1);
insert into akiba_information_schema.index_columns values ('toy_test', 'customers', 'PRIMARY', 'customer_id', 0, 1);
insert into akiba_information_schema.index_columns values ('toy_test', 'items', 'item_id', 'item_id', 0, 1);
insert into akiba_information_schema.index_columns values ('toy_test', 'items', 'order_id', 'order_id', 0, 1);
insert into akiba_information_schema.index_columns values ('toy_test', 'items', 'PRIMARY', 'item_id', 0, 1);
insert into akiba_information_schema.index_columns values ('toy_test', 'orders', 'customer_id', 'customer_id', 0, 1);
insert into akiba_information_schema.index_columns values ('toy_test', 'orders', 'order_id', 'order_id', 0, 1);
insert into akiba_information_schema.index_columns values ('toy_test', 'orders', 'PRIMARY', 'order_id', 0, 1);
insert into akiba_information_schema.index_columns values ('akiba_objects', '_akiba_coi', '_akiba_coi$customers_PK', 'customers$customer_id', 0, 1);
insert into akiba_information_schema.index_columns values ('akiba_objects', '_akiba_coi', '_akiba_coi$items_PK', 'items$item_id', 0, 1);
insert into akiba_information_schema.index_columns values ('akiba_objects', '_akiba_coi', '_akiba_coi$orders_PK', 'orders$order_id', 0, 1);
insert into akiba_information_schema.index_columns values ('akiba_objects', '_akiba_coi', 'customers$customer_id', 'customers$customer_id', 0, 1);
insert into akiba_information_schema.index_columns values ('akiba_objects', '_akiba_coi', 'items$item_id', 'items$item_id', 0, 1);
insert into akiba_information_schema.index_columns values ('akiba_objects', '_akiba_coi', 'items$order_id', 'items$order_id', 0, 1);
insert into akiba_information_schema.index_columns values ('akiba_objects', '_akiba_coi', 'orders$customer_id', 'orders$customer_id', 0, 1);
insert into akiba_information_schema.index_columns values ('akiba_objects', '_akiba_coi', 'orders$order_id', 'orders$order_id', 0, 1);

create table `akiba_objects`.`_akiba_coi`(
  `customers$customer_id` int,
  `orders$order_id` int,
  `orders$customer_id` int,
  `items$item_id` int,
  `items$order_id` int,
  INDEX  `_akiba_coi$customers_PK` (`customers$customer_id`),
  INDEX  `_akiba_coi$items_PK` (`items$item_id`),
  INDEX  `_akiba_coi$orders_PK` (`orders$order_id`),
  INDEX  `customers$customer_id` (`customers$customer_id`),
  INDEX  `items$item_id` (`items$item_id`),
  INDEX  `items$order_id` (`items$order_id`),
  INDEX  `orders$customer_id` (`orders$customer_id`),
  INDEX  `orders$order_id` (`orders$order_id`)
) engine=akibadb;
