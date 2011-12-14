CREATE TABLE parent
(
  id INT, PRIMARY KEY(id), 
  name VARCHAR(256) NOT NULL, KEY(name)
) engine=akibandb;

CREATE TABLE child
(
  id INT, PRIMARY KEY(id), 
  pid INT, CONSTRAINT `__akiban_fk_10` FOREIGN KEY `__akiban_fk_10`(pid) REFERENCES parent(id), 
  name VARCHAR(256) NOT NULL
) engine=akibandb;
