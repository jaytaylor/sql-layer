SELECT * FROM t1, t2, t3, t4, t5 
 WHERE t1.c1 = t2.c1
   AND t3.c2 = t4.c2
   AND t1.c3 + t2.c3 + t3.c3 = t4.c3 + t5.c3
