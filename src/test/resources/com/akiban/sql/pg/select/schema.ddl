
CREATE TABLE customers
(
  cid int NOT NULL auto_increment, 
  PRIMARY KEY(cid),
  name varchar(32) NOT NULL,
  KEY(name)
) engine=akibandb;

CREATE TABLE orders
(
  oid int NOT NULL auto_increment, 
  PRIMARY KEY(oid),
  cid int NOT NULL,
  order_date date NOT NULL,
  KEY(order_date),
  CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (cid) REFERENCES customers(cid)
) engine=akibandb;

CREATE TABLE items
(
  iid int NOT NULL auto_increment, 
  PRIMARY KEY(iid),
  oid int NOT NULL,
  sku varchar(32) NOT NULL,
  KEY(sku),
  quan int NOT NULL,
  CONSTRAINT `__akiban_fk_1` FOREIGN KEY `__akiban_fk_1` (oid) REFERENCES orders(oid)
) engine=akibandb;

CREATE TABLE types
(
  a_int int PRIMARY KEY,
  a_uint int unsigned,
  a_float float,
  a_ufloat float unsigned,
  a_double double,
  a_udouble double unsigned,
  a_decimal decimal(5,2),
  a_udecimal decimal(5,2) unsigned,
  a_varchar varchar(16),
  a_date date,
  a_time time,
  a_datetime datetime,
  a_timestamp timestamp,
  a_year year,
  a_text text
) engine=akibandb;
