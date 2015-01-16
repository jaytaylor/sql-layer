CREATE TABLE places
(
  pid int NOT NULL PRIMARY KEY, 
  state CHAR(2),
  city VARCHAR(100),
  shape BLOB
);

CREATE INDEX places_geo ON places(geo_lat_lon(shape));
