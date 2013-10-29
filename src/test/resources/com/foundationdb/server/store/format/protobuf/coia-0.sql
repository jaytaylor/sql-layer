
CREATE TABLE customers
(
  cid int NOT NULL, 
  PRIMARY KEY(cid),
  name varchar(32) NOT NULL
);

CREATE TABLE orders
(
  oid int NOT NULL, 
  PRIMARY KEY(oid),
  cid int NOT NULL,
  order_date date NOT NULL,
  GROUPING FOREIGN KEY (cid) REFERENCES customers(cid)
);

CREATE TABLE items
(
  iid int NOT NULL, 
  PRIMARY KEY(iid),
  oid int NOT NULL,
  sku varchar(32) NOT NULL,
  quan int NOT NULL,
  GROUPING FOREIGN KEY (oid) REFERENCES orders(oid)
);

CREATE TABLE addresses
(
  aid int NOT NULL, 
  PRIMARY KEY(aid),
  cid int NOT NULL,
  state CHAR(2),
  city VARCHAR(100),
  GROUPING FOREIGN KEY (cid) REFERENCES customers(cid)
);
