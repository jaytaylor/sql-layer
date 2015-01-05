CREATE TABLE parent(id INT NOT NULL, PRIMARY KEY(id), name VARCHAR(256) NOT NULL, UNIQUE(name));
CREATE TABLE child(id INT NOT NULL, PRIMARY KEY(id), pid INT, GROUPING FOREIGN KEY (pid) REFERENCES parent(id), name VARCHAR(256) NOT NULL, CONSTRAINT pid_and_name UNIQUE(pid,name));
CREATE VIEW names(pname,cname) AS SELECT parent.name,child.name FROM parent,child WHERE parent.id = child.pid;
CREATE TABLE t1(id INT NOT NULL, c1 VARCHAR(50));