join-fk-1 : simple fk only join

join-fk-2 : three way FK join (parent->child->grandchild)

join-fk-3 : two joins - child to same parent twice

join-fk-4 : two joins - parent to two children

join-fk-5: two joins - child with two parents

join-fk-6: two joins - child with two parents, opposite join order from join-fk-5

join-gfk-1 : simple GFK join

join-gfk-2 : GFK + FK join 

join-multi-fk-1 : simple multi-column FK join

join-multi-fk-2 : negative test, multi-column fk using one column, no FK flagged

join-multi-fk-3 : multi-column fk, plus join to another table

join-multi-fk-4 : negative test, multi-fk using one column, plus join to another table, no FK flagged
