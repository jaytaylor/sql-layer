
/*
schema data_dictionary_test;

group coi {
  table customer {
    table address(customer_id),
    table order (customer_id) {
      table item (order_id) {
        table component(part_id)
      }
    }
  }
};

*/

create table customer(
    customer_id bigint not null,
    customer_name varchar(100) not null,
    primary key(customer_id)
) engine = akibadb;

create table `order`(
    order_id bigint not null,
    customer_id bigint not null,
    order_date int not null,
    primary key(order_id),
    foreign key(customer_id) references customer
) engine = akibadb;

create table `address`(
    customer_id bigint not null,
    instance_id int not null,
    address_line1 varchar(60) not null,
    address_line2 varchar(60) not null,
    address_line3 varchar(60) not null,
    primary key (`instance_id`)
) engine = akibadb;

create table item(
    order_id bigint not null,
    part_id bigint not null,
    quantity int not null,
    unit_price int not null,
    primary key(order_id, part_id),
    foreign key(order_id) references `order`
) engine = akibadb;

create table component(
    part_id bigint not null,
    component_id bigint not null,
    supplier_id int not null,
    unique_id int not null,
    description varchar(50),
    primary key (`component_id`),
    foreign key `fk` (`part_id`),
    unique key `uk` (`unique_id`),
    key `xk` (supplier_id)
) engine = akibadb;
