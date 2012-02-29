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

CREATE TABLE blogs
(
  bid1 int NOT NULL,
  bid2 int NOT NULL,
  PRIMARY KEY(bid1, bid2),
  title varchar(56),
  bcol1 int,
  bcol2 int
) engine=akibandb;

CREATE TABLE comments
(
  cid int NOT NULL auto_increment,
  PRIMARY KEY(cid),
  bfk1 int NOT NULL,
  bfk2 int NOT NULL,
  content varchar(56),
  ccol1 int,
  ccol2 int,
  CONSTRAINT `__akiban_fk_3` FOREIGN KEY `__akiban_fk_3` (bfk1, bfk2) REFERENCES blogs(bid1, bid2)
) engine=akibandb;