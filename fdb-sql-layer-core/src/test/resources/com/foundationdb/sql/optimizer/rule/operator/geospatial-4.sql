SELECT p.pid
FROM places p
LEFT JOIN food_vendors f on f.pid = p.pid
 WHERE p.state = 'MA'
   AND f.name = 'starbucks'
   AND distance_lat_lon(p.lat, p.lon, 42.3583, -71.0603) <= 0.0466
 ORDER BY distance_lat_lon(p.lat, p.lon, 42.3583, -71.0603)
