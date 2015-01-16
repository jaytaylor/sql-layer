CREATE TABLE places
(
  pid int NOT NULL PRIMARY KEY, 
  state CHAR(2),
  city VARCHAR(100),
  lat DECIMAL(8,4),
  lon DECIMAL(8,4)
);

CREATE TABLE food_vendors(
  vid int NOT NULL PRIMARY KEY,
  pid int,
  name varchar(100),
  grouping foreign key(pid) references places(pid)
);

CREATE INDEX name_geo ON food_vendors(
    places.state,
    food_vendors.name,
    geo_lat_lon(places.lat, places.lon),
    places.lat,
    places.lon
) using left join;
