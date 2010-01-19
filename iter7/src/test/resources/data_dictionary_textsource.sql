--% schema data_dictionary_test
--% group coi
--% table customer
--% table .order customer_id
--% table ..item order_id

create table `customer` (
    `customer_id` int(11) not null,
    `customer_name` varchar(100) not null,
    primary key(`customer_id`)
) engine=akibadb;

create table `order` (
    `order_id` int(11) not null,
    `customer_id` int(11) not null,
    `order_date` int(11) not null,
    primary key(`order_id`),
    foreign key(`customer_id`) references customer
) engine=akibadb;

create table `item` (
    `order_id` int(11) not null,
    `part_id` int(11) not null,
    `quantity` int(11) not null,
    `unit_price` int(11) not null,
    primary key(`part_id`),
    foreign key(`order_id`) references `order`
) engine=akibadb;



