CREATE TABLE people(
    pid INT NOT NULL PRIMARY KEY,
    name VARCHAR(32) COLLATE utf8_bin
);

CREATE TABLE addresses(
    aid INT NOT NULL PRIMARY KEY,
    pid INT NOT NULL,
    street VARCHAR(32) COLLATE utf8_bin,
    GROUPING FOREIGN KEY (pid) REFERENCES people (pid)
);

CREATE INDEX name_address ON addresses (addresses.street, people.name) USING LEFT JOIN;
