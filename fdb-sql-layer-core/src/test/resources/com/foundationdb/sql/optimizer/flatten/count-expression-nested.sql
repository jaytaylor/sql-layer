-- The middle SELECT here can't be flattened out into the COUNT() because it has an expression, but the inner most
-- SELECT can be flattened out into the middle one
SELECT COUNT(val) FROM (SELECT id + 3 AS val FROM (SELECT id FROM t1) AS anon1) AS anon2