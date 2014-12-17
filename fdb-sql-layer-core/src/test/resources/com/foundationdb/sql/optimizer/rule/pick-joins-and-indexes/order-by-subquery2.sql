-- Make sure sort is handled for both inside and outside
SELECT * FROM (SELECT id FROM a ORDER BY id LIMIT 10) AS anon1 JOIN m2m on anon1.id = m2m.aid ORDER BY m2m.id