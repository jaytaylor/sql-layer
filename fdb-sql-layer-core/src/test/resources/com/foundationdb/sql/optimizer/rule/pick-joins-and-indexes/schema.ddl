CREATE TABLE parent(id INT NOT NULL, PRIMARY KEY(id), name VARCHAR(256) NOT NULL, UNIQUE(name), state CHAR(2));
CREATE TABLE child(id INT NOT NULL, PRIMARY KEY(id), pid INT, GROUPING FOREIGN KEY(pid) REFERENCES parent(id), name VARCHAR(256) NOT NULL);

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
  order_date date NOT NULL,
  GROUPING FOREIGN KEY (cid) REFERENCES customers(cid)
);
CREATE INDEX "__akiban_fk_0" ON orders(cid);
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
CREATE INDEX "__akiban_fk_1" ON items(oid);
CREATE INDEX sku ON items(sku);

CREATE TABLE addresses
(
  aid int NOT NULL, 
  PRIMARY KEY(aid),
  cid int NOT NULL,
  state CHAR(2),
  city VARCHAR(100),
  GROUPING FOREIGN KEY (cid) REFERENCES customers(cid)
);
CREATE INDEX "__akiban_fk_2" ON addresses(cid);
CREATE INDEX state ON addresses(state);

CREATE TABLE categories
(
   cat int NOT NULL,
   sku varchar(32) NOT NULL
);
CREATE UNIQUE INDEX cat_sku ON categories(cat,sku);

CREATE TABLE sources
(
   country CHAR(3) NOT NULL,
   sku varchar(32) NOT NULL
);
CREATE INDEX source_country ON sources(country);

CREATE INDEX cname_and_sku ON customers(customers.name, items.sku) USING LEFT JOIN;
CREATE INDEX sku_and_date ON customers(items.sku, orders.order_date) USING LEFT JOIN;
CREATE INDEX state_and_name ON customers(addresses.state, customers.name) USING RIGHT JOIN;

CREATE INDEX cust_ft ON customers(FULL_TEXT(name, items.sku, addresses.state));

CREATE TABLE primary1(c1 INT PRIMARY KEY, c2 INT, c3 INT);

-- NO Indexes or groups or anything
CREATE TABLE t1(c1 INT, c2 INT, c3 INT);
CREATE TABLE t2(c1 INT, c2 INT, c3 INT);
CREATE TABLE t3(c1 INT, c2 INT, c3 INT);
