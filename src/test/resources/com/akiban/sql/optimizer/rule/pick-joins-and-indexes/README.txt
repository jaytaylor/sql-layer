group-equals: two equals on group index

in-index: IN that can use index

in-sorted: IN that needs sorting

equals-sorted: Same with = for comparison.

in-subquery: IN SELECT to semi-join (not indexable).

join-across-subquery: group join in subquery expression

join-cond-too-complex: A join condition that will not work with a group join.

right-too-complex-1: RIGHT join condition on child

right-too-complex-2: RIGHT join condition on parent

single-subquery: just derived table (should push down condition)

single-table: whole row from indexed table

subquery: join to subquery

three-groups: second group dividing two halves

two-groups: group and non-group joins

two-groups-indexed: two groups with condition on one
