-- Technically this can flatten anon2, but managing that requires maintaining which subqueries can be flattened
-- according to outer aggregates, which doesn't seem worth it right now.
SELECT bit_and(a + b) FROM (SELECT COUNT(*) AS a FROM t1) AS anon1, (SELECT id AS b FROM t1) AS anon2