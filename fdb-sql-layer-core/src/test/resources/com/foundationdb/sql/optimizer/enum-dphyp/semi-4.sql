-- This differs from semi-3 in that the conditions are in the ON
-- In Semi-3 it moves the conditions onto the semi-join and nothing gets confused
-- but here, the semi join has no conditions in dphyp
-- Just in case this test started failing again because the inner join is being moved outside, here is why that is never
-- correct. If the outer join returns multiple rows for a given row of t1, then you will get multiple copies of that in
-- the results, instead of just 1.
SELECT t1.c1, t1.c2 FROM t1 WHERE EXISTS (
  SELECT 1 FROM t2 JOIN t3
    ON t1.c1 = t2.c1 AND t3.c1 = t2.c2 AND t3.c2 = 7)
  ORDER BY t1.c2
