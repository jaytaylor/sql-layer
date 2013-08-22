SELECT city, state FROM places
 WHERE distance_lat_lon(lat, lon, 42.3583, -71.0603) <= 0.0466
 ORDER BY distance_lat_lon(lat, lon, 42.3583, -71.0603)
