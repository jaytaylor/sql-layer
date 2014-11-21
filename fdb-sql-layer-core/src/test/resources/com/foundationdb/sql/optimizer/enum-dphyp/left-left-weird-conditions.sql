-- the following query is the same as:
--   If t2 is empty, every row in t1, with nulls
--   Else (t1 cross t2) left t3 on t1.c1 = t2.c1 and t1.c2 = t3.c2
select * from t1 left join t2 on true left join t3 on t1.c1 = t2.c1 and t1.c2 = t3.c2
