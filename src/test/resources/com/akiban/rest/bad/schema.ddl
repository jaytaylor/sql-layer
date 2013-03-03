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

CREATE PROCEDURE test_proc(IN x DOUBLE, IN y DOUBLE, OUT plus DOUBLE, OUT times DOUBLE) LANGUAGE javascript PARAMETER STYLE variables DYNAMIC RESULT SETS 1  AS $$
  plus = x + y;
  times = x * y;
  java.sql.DriverManager.getConnection("jdbc:default:connection").createStatement().executeQuery("SELECT first_name, COUNT(*) AS n FROM test.customers GROUP BY 1");
$$;
