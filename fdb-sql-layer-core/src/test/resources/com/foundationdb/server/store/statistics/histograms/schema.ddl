CREATE TABLE people(
    pid INT NOT NULL PRIMARY KEY,
    name VARCHAR(32) COLLATE UCS_BINARY
);

CREATE TABLE addresses(
    aid INT NOT NULL PRIMARY KEY,
    pid INT NOT NULL,
    street VARCHAR(32) COLLATE UCS_BINARY,
    GROUPING FOREIGN KEY (pid) REFERENCES people (pid)
);

CREATE INDEX name_address ON addresses (addresses.street, people.name) USING LEFT JOIN;
