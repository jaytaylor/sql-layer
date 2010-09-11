use `data_dictionary_test`;

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
    foreign key(customer_id) references customer(customer_id),
CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`customer_id`) REFERENCES `customer` (`customer_id`)
) engine = akibadb;

create table `address`(
    customer_id bigint not null,
    instance_id int not null,
    address_line1 varchar(60) not null,
    address_line2 varchar(60) not null,
    address_line3 varchar(60) not null,
    primary key (`customer_id`, `instance_id`),
CONSTRAINT `__akiban_fk_1` FOREIGN KEY `__akiban_fk_1` (`customer_id`) REFERENCES `customer` (`customer_id`)
) engine = akibadb;

create table item(
    order_id bigint not null,
    part_id bigint not null,
    quantity int not null,
    unit_price int not null,
    primary key(part_id),
    foreign key(order_id) references `order`(order_id),
CONSTRAINT `__akiban_fk_2` FOREIGN KEY `__akiban_fk_2` (`order_id`) REFERENCES `order` (`order_id`)
) engine = akibadb;

create table component(
    part_id bigint not null,
    component_id bigint not null,
    supplier_id int not null,
    unique_id int not null,
    description varchar(50),
    primary key (`component_id`),
    foreign key `fk` (`part_id`) references item(`part_id`),
    unique key `uk` (`unique_id`),
    key `xk` (supplier_id),
CONSTRAINT `__akiban_fk_3` FOREIGN KEY `__akiban_fk_3` (`part_id`) REFERENCES `item` (`part_id`)
) engine = akibadb;
