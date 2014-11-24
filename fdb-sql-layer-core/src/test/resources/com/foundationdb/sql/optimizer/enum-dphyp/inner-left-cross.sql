-- Complicated much. This adds some tables so that there is stuff around and mixed in with our cross product on the
-- right side of the left join.
-- Note that T4 is completely unconnected, but it get's the cross product done inside the left join.
-- The outer joins are there to make sure that tables are floating around the left join, which affects the dhyper
-- algorithm
SELECT T1.c1
FROM (T2 LEFT JOIN (T3 JOIN T4 ON T2.c2 = T3.c2) ON T2.c1 = T3.c1) JOIN T1 ON T1.c2 = T2.c2

