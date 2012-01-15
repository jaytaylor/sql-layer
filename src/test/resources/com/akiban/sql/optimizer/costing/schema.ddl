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
  price decimal(6,2) NOT NULL,
  CONSTRAINT `__akiban_fk_1` FOREIGN KEY `__akiban_fk_1` (oid) REFERENCES orders(oid)
) engine=akibandb;

CREATE TABLE addresses
(
  aid int NOT NULL auto_increment, 
  PRIMARY KEY(aid),
  cid int NOT NULL,
  state CHAR(2),
  KEY(state),
  city VARCHAR(100),
  CONSTRAINT `__akiban_fk_2` FOREIGN KEY `__akiban_fk_2` (cid) REFERENCES customers(cid)
) engine=akibandb;
