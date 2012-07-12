
CREATE TABLE customers
(
  cid int NOT NULL, 
  PRIMARY KEY(cid),
  name varchar(32) NOT NULL
);
CREATE INDEX name ON customers(name);

CREATE TABLE orders
(
  oid int NOT NULL, 
  PRIMARY KEY(oid),
  cid int NOT NULL,
  order_date date,
  GROUPING FOREIGN KEY (cid) REFERENCES customers(cid)
);
CREATE INDEX order_date ON orders(order_date);

CREATE TABLE items
(
  iid int NOT NULL, 
  PRIMARY KEY(iid),
  oid int NOT NULL,
  sku varchar(32) NOT NULL,
  quan int NOT NULL,
  GROUPING FOREIGN KEY (oid) REFERENCES orders(oid)
);
CREATE INDEX sku ON items(sku);

CREATE TABLE log
(
  cid int not null,
  event_date int not null,
  what varchar(20) not null,
  GROUPING FOREIGN KEY (cid) REFERENCES customers(cid)
);
