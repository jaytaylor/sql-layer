-- This was inspired by reverse-and-cleanup-ins/map-join-project-outer, but that one has a right join;
-- one of the rules (I think OuterJoinPromoter) turns all right joins to left joins before hitting
-- JoinAndIndexPicker

-- Also, this is super restrictive, nothing can be moved around.
SELECT t1.c1 FROM t3
        LEFT JOIN (t1
        LEFT JOIN t2 ON t1.c2 = t2.c2)  ON t2.c2 = t3.c2
 WHERE EXISTS (SELECT * FROM t4 WHERE t4.c2 = t1.c2)

