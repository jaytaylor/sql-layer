-- Make sure sort is handled for both inside and outside
-- The subquery should be able to provide the sort for the outside, but that's a lot of work. For now, just ensure
-- that the sort still exists. Ideally it would put the subquery as the outer source and not have run an additional
-- sort
SELECT * FROM t1 JOIN (SELECT id FROM a ORDER BY id LIMIT 10) AS anon1 on anon1.id = t1.c1 ORDER BY anon1.id
