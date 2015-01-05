-- originally ran into a much more complicated version of this (lots of conditons) with the RandomSemiJoinTestDT
-- but that query made no sense, because you did an exists and in the right side did a cross product, the second
-- table of the cross product had absolutely no impact on the results (other than breaking the optimizer). This
-- query on the other hand somewhat close to reasonable to have that cross join on the right side of the left
-- join.
SELECT ta0.c1
FROM t2 AS ta0
LEFT OUTER JOIN
(t1 JOIN t3 ON ta0.c1 = t1.c3)
ON ta0.c2 = t1.c1

