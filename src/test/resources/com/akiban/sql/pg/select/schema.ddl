
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
CREATE INDEX cid ON orders(cid);
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

CREATE TABLE addresses
(
  aid int NOT NULL, 
  PRIMARY KEY(aid),
  cid int NOT NULL,
  state CHAR(2),
  city VARCHAR(100),
  GROUPING FOREIGN KEY (cid) REFERENCES customers(cid)
);

--
-- Moved into PostgresServerSelectIT due to type requirements
--
-- CREATE TABLE types
-- (
--   a_int int PRIMARY KEY,
--   a_uint int unsigned,
--   a_float float,
--   a_ufloat float unsigned,
--   a_double double,
--   a_udouble double unsigned,
--   a_decimal decimal(5,2),
--   a_udecimal decimal(5,2) unsigned,
--   a_varchar varchar(16),
--   a_date date,
--   a_time time,
--   a_datetime datetime,
--   a_timestamp timestamp,
--   a_year year,
--   a_text text
-- );
-- 
-- CREATE TABLE types_i
-- (
--   a_int int, PRIMARY KEY(a_int),
--   a_uint int unsigned, INDEX(a_uint),
--   a_float float, INDEX(a_float),
--   a_ufloat float unsigned, INDEX(a_ufloat),
--   a_double double, INDEX(a_double),
--   a_udouble double unsigned, INDEX(a_udouble),
--   a_decimal decimal(5,2), INDEX(a_decimal),
--   a_udecimal decimal(5,2) unsigned, INDEX(a_udecimal),
--   a_varchar varchar(16), INDEX(a_varchar),
--   a_date date, INDEX(a_date),
--   a_time time, INDEX(a_time),
--   a_datetime datetime, INDEX(a_datetime),
--   a_timestamp timestamp, INDEX(a_timestamp),
--   a_year year, INDEX(a_year)
-- );
