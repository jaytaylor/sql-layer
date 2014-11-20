-- Note that the condition in the right join only references the tables in the
-- left join.
select * from t1 left join t2 on t1.c2 = t2.c2 right join t3 on t1.c1 = t2.c1 and t2.c2 = t3.c2