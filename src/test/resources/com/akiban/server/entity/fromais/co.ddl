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
CREATE UNIQUE INDEX idx_n ON customers(name);
CREATE INDEX idx_p ON orders(orders.placed, customers.name) USING LEFT JOIN;