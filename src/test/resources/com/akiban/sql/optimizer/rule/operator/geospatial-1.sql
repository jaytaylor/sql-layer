SELECT MAX(distance_lat_lon(lat, lon, 42.3583, -71.0603)) 
  FROM (SELECT lat, lon FROM places
      ORDER BY znear(lat, lon, 42.3583, -71.0603)
         LIMIT 10) zp