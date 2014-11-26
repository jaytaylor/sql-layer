-- This is the other option for onesided conditions.
-- primary1.c1=80 should be pushed down to the primary1 scan
SELECT *
FROM t1
WHERE NOT EXISTS
    (SELECT 1
     FROM primary1
     WHERE primary1.c1 = 80
     AND primary1.c2=t1.c2)