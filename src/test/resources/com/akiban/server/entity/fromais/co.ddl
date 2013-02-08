CREATE TABLE customers (
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR(32));
CREATE TABLE orders(
    oid_1 INT NOT NULL,
    oid_2 VARCHAR(56) NOT NULL,
    cid INT,
    placed DATE,
    PRIMARY KEY(oid_1, oid_2),
    GROUPING FOREIGN KEY (cid) REFERENCES customers(id));