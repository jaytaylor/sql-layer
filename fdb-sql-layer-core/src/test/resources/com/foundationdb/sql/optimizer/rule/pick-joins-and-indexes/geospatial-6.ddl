CREATE TABLE places
(
  pid int NOT NULL PRIMARY KEY, 
  state CHAR(2),
  city VARCHAR(100),
  shape BLOB
);

CREATE INDEX places_geo ON places(z_order_lat_lon(shape));
