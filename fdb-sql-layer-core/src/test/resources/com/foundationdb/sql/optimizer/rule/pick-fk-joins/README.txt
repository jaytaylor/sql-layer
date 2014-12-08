join-fk-1 : simple fk only join

join-fk-2 : three way FK join (parent->child->grandchild)

join-fk-3 : two joins - child to same parent twice

join-fk-4 : two joins - parent to two children

join-fk-5: two joins - child with two parents

join-fk-6: two joins - child with two parents, opposite join order from join-fk-5

join-fk-7: simple left FK join

join-fk-8: simple join three join with child->gparent column join

join-fk-9: left outer three join with child->gparent column join

join-fk-10: As 9, but with added condition to check condition processing correctly.

join-gfk-1 : simple GFK join

join-gfk-2 : GFK + FK join 

join-long-1 : long (11+ tables) join query 

join-long-2 : long (11+ tables) left outer join query

join-long-3 : long query with sub-query and table w/no primary key 

join-multi-fk-1 : simple multi-column FK join

join-multi-fk-2 : negative test, multi-column fk using one column, no FK flagged

join-multi-fk-3 : multi-column fk, plus join to another table

join-multi-fk-4 : negative test, multi-fk using one column, plus join to another table, no FK flagged

join-multi-fk-5 : negative test, mulit-fk with join columns reversed