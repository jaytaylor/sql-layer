CREATE TABLE parent(id INT, PRIMARY KEY(id), name VARCHAR(256) NOT NULL, UNIQUE(name), state CHAR(2)) engine=akibandb;
CREATE TABLE child(id INT, PRIMARY KEY(id), pid INT, CONSTRAINT `__akiban_fk_10` FOREIGN KEY `__akiban_fk_10`(pid) REFERENCES parent(id), name VARCHAR(256) NOT NULL) engine=akibandb;

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

CREATE TABLE books
(
  bid int NOT NULL auto_increment, 
  name VARCHAR(50) NOT NULL, 
  copyright YEAR, 
  author VARCHAR(50) NOT NULL,
  KEY(copyright,author,name)
) engine=akibandb;
