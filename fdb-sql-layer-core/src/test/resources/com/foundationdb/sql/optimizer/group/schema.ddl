CREATE TABLE parent(id INT NOT NULL, PRIMARY KEY(id), name VARCHAR(256) NOT NULL, UNIQUE(name), state CHAR(2));
CREATE TABLE child(id INT NOT NULL, PRIMARY KEY(id), pid INT, GROUPING FOREIGN KEY(pid) REFERENCES parent(id), name VARCHAR(256) NOT NULL);
CREATE VIEW names(pname,cname) AS SELECT parent.name,child.name FROM parent,child WHERE parent.id = child.pid;

-- Some ungrouped tables
CREATE TABLE t1(c1 INT, c2 INT, c3 INT);
CREATE TABLE t2(c1 INT, c2 INT, c3 INT);
CREATE TABLE t3(c1 INT, c2 INT, c3 INT);
CREATE TABLE t4(c1 INT, c2 INT, c3 INT);
CREATE TABLE t5(c1 INT, c2 INT, c3 INT);
CREATE TABLE t6(c1 INT, c2 INT, c3 INT);
