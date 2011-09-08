CREATE TABLE parent(id INT, PRIMARY KEY(id), name VARCHAR(256) NOT NULL, UNIQUE(name), state CHAR(2));
CREATE TABLE child(id INT, PRIMARY KEY(id), pid INT, CONSTRAINT `__akiban_fk0` FOREIGN KEY akibanfk(pid) REFERENCES parent(id), name VARCHAR(256) NOT NULL);
