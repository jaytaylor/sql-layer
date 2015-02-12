CREATE TABLE places
(
  pid int NOT NULL PRIMARY KEY, 
  state CHAR(2),
  city VARCHAR(100),
  lat DECIMAL(8,4),
  lon DECIMAL(8,4)
);

CREATE INDEX places_geo_1 ON places(state, geo_lat_lon(lat, lon), lat, lon);
CREATE INDEX places_geo_2 ON places(city, geo_lat_lon(lat, lon), lat, lon);
