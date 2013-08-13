CREATE TABLE customers(
    cid INT NOT NULL,
    first_name varchar(32),
    last_name varchar(32),
    PRIMARY KEY(cid)
);

CREATE TABLE addresses
(
  aid int NOT NULL,
  cid int NOT NULL,
  state CHAR(2),
  city VARCHAR(100),
  PRIMARY KEY(aid),
  GROUPING FOREIGN KEY (cid) REFERENCES customers(cid)
);

CREATE TABLE orders(
    oid INT NOT NULL,
    cid INT,
    odate DATETIME,
    PRIMARY KEY(oid),
    GROUPING FOREIGN KEY(cid) REFERENCES customers(cid)
);

CREATE TABLE items(
    iid INT NOT NULL,
    oid INT,
    sku INT,
    PRIMARY KEY(iid),
    GROUPING FOREIGN KEY(oid) REFERENCES orders(oid)
);

CREATE VIEW ca_view AS SELECT last_name, first_name, city FROM test.customers INNER JOIN test.addresses on (customers.cid = addresses.cid);

CREATE VIEW test.coi_view AS SELECT first_name, odate, sku FROM test.customers INNER JOIN test.orders on (customers.cid = orders.cid) INNER JOIN test.items on (orders.oid = items.oid);