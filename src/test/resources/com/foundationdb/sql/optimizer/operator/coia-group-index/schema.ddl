
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

CREATE TABLE referrals
(
  rid int NOT NULL, 
  PRIMARY KEY(rid),
  cid int NOT NULL,
  source VARCHAR(256),
  referral VARCHAR(256),
  GROUPING FOREIGN KEY (cid) REFERENCES customers(cid)
);

CREATE INDEX cname_and_sku ON customers(customers.name, items.sku) USING LEFT JOIN;
CREATE INDEX sku_and_date ON customers(items.sku, orders.order_date) USING LEFT JOIN;
CREATE INDEX state_and_cname ON customers(addresses.state, customers.name) USING LEFT JOIN;
