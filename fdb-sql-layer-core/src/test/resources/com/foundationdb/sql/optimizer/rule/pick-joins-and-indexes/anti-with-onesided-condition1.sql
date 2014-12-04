-- Need to make sure that the primary1.c1=80 doesn't end up being considered for the primary1
-- part of the plan. Although we could be smart enough to detect that it needs to
-- be negated....
-- could be rewritten
-- SELECT * FROM t1 WHERE not exists (select 1 from t2 where t1.c2=t2.c2) or not t1.c1 = 80 or t1.c1 is null
SELECT *
FROM primary1
WHERE NOT EXISTS
    (SELECT 1
     FROM t2
     WHERE primary1.c1 = 80
     AND primary1.c2=t2.c2)