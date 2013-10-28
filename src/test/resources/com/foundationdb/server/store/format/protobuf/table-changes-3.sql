
CREATE TABLE addresses
(
  aid int NOT NULL, 
  PRIMARY KEY(aid),
  cid int NOT NULL,
  region VARCHAR(8),
  city VARCHAR(100),
  GROUPING FOREIGN KEY (cid) REFERENCES customers(cid)
);
