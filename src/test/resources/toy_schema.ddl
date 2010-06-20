
/*
schema toy_test baseid=0;

group coi {
  table customers {
    table orders (customer_id) {
      table items (order_id)
    }
  }
};

*/

create table customers 
	(
	 customer_id int not null default '0',
	 primary key(customer_id),
	 key (customer_id)
        ) engine=akibadb;

create table orders
	(
	 order_id int not null default '0',
	 primary key(order_id),
	 key (order_id),
	 customer_id int, 
	 key (customer_id)
	) engine=akibadb;

create table items
	(
	 item_id int not null default '0',
	 primary key(item_id),
	 key (item_id),
	 order_id int,
	 key (order_id)
	) engine=akibadb;


