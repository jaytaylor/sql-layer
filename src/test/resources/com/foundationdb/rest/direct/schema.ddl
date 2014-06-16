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

CREATE PROCEDURE test_proc(IN x DOUBLE, IN y DOUBLE, OUT plus DOUBLE, OUT times DOUBLE) LANGUAGE javascript PARAMETER STYLE variables DYNAMIC RESULT SETS 1  AS $$
  plus = x + y;
  times = x * y;
  java.sql.DriverManager.getConnection("jdbc:default:connection").createStatement().executeQuery("SELECT first_name, COUNT(*) AS n FROM test.customers GROUP BY 1");
$$;

CREATE PROCEDURE test_json(IN json_in VARCHAR(4096), OUT json_out VARCHAR(4096)) LANGUAGE javascript PARAMETER STYLE json AS $$
  var params = JSON.parse(json_in);
  var extent = com.foundationdb.direct.Direct.context.extent;

  // There is only an accessor for root classes (customers, not orders).
  // There is no accessor with primary key in extent (only in children).
  // There is no List-like access.
  // Iterator next() only works after hasNext().
  var cname = null;
  if (typeof Iterator == 'undefined') {
    for each (var c in extent.customers) {
      cname = c.firstName + " " + c.lastName;
      break;
    }
  }
  else {
    for each (var c in Iterator(extent.customers.iterator())) {
      cname = c.firstName + " " + c.lastName;
      break;
    }
  }
  var oids = params.oids;
  var result = { orders: [] };
  for (var i = 0; i < oids.length; i++) {
    var entry = { oid: oids[i], name: cname };
    result.orders.push(entry);
  }
  JSON.stringify(result)
$$;