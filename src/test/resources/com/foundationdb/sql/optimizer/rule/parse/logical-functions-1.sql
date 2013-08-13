SELECT COALESCE(name, 'N/A') FROM parent
 WHERE id != 1 OR id > 10 AND id < 20 AND NOT (id = 3)
   AND (name IS NULL OR name = '')
   AND (state IS NOT NULL or NOT state IS NULL)