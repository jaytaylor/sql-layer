CREATE TABLE parent
(
  id INT NOT NULL, PRIMARY KEY(id), 
  name VARCHAR(256) NOT NULL,
  name_sv VARCHAR(32) COLLATE latin1_swedish_ci
);
CREATE INDEX name ON parent(name);
CREATE INDEX name_sv ON parent(name_sv);

CREATE TABLE child
(
  id INT NOT NULL, PRIMARY KEY(id), 
  pid INT, GROUPING FOREIGN KEY (pid) REFERENCES parent(id), 
  value DECIMAL(4,2)
);

CREATE INDEX value ON parent(parent.name, child.value) USING LEFT JOIN;
