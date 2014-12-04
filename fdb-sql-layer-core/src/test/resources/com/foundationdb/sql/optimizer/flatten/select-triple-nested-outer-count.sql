-- The middle SELECT here can't be flattened out into the COUNT() because it has an expression, but the inner most
-- SELECT can be flattened out into the middle one
SELECT COUNT(mid) FROM (SELECT iid * 5 AS mid FROM (SELECT id-10 AS iid FROM parent WHERE id > 3) AS innerSelect WHERE iid < 5) AS midselect WHERE mid % 2 = 0
