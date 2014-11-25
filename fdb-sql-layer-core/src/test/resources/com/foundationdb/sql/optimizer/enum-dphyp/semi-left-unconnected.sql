-- this one does a semi join, but doesn't actually connect the two sides
-- this is similar to left-left-weird-conditions
-- it basically amounts to:
--   If t2 is empty, return the empty set
--   Else: SELECT t1.c1 FROM t1 WHERE t1.c1 = 5
--
-- Note, it probably wouldn't be absurd to add another rule to handle dumb
-- queries like this, and then make it forbidden for it to hit dphyp

SELECT t1.c1
FROM t1
WHERE t1.c1 = 5
  AND EXISTS
    (SELECT 1
     FROM t2 LEFT JOIN t3 ON t2.c1=t3.c1)
