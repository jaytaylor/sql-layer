CREATE TABLE parent
(
  pid int NOT NULL, 
  x int,
  filler varchar(100),
  PRIMARY KEY(pid)
);

CREATE TABLE child
(
  cid int NOT NULL,
  pid int,
  y int,
  PRIMARY KEY(cid),
  GROUPING FOREIGN KEY (pid) REFERENCES parent(pid)
);

CREATE INDEX px ON parent(x);
CREATE INDEX cy ON child(y);
CREATE INDEX cp ON child(pid);
