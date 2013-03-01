CREATE TABLE customers(
    cid INT NOT NULL,
    name varchar(256) COLLATE en_us_ci,
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
    sku VARCHAR(8),
    PRIMARY KEY(iid),
    GROUPING FOREIGN KEY(oid) REFERENCES orders(oid)
);

CREATE INDEX c_ai ON customers(FULL_TEXT(name, addresses.state, items.sku));
