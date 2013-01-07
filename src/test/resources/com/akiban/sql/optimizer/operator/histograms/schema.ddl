CREATE TABLE t
( 
  id int NOT NULL,
  x int,
  y int,
  z int,
  PRIMARY KEY(id)
);

CREATE INDEX idx_txy ON t(x, y);
CREATE INDEX idx_tz ON t(z);
