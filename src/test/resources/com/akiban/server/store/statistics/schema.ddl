CREATE TABLE parent
(
  id INT NOT NULL, PRIMARY KEY(id), 
  name VARCHAR(256) NOT NULL
);
CREATE INDEX name ON parent(name);

CREATE TABLE child
(
  id INT NOT NULL, PRIMARY KEY(id), 
  pid INT, GROUPING FOREIGN KEY (pid) REFERENCES parent(id), 
  name VARCHAR(256) NOT NULL
);

CREATE INDEX names ON parent(parent.name, child.name) USING LEFT JOIN;
