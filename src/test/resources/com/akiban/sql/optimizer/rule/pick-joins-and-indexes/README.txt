choose-condition-1: narrow and wide

choose-condition-2: wide and narrow

covering-or-group-scan: covering index or group scan?

distinct-sorted: DISTINCT + ORDER BY from index.

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

subquery: join to subquery (with condition pushed down and combined into index)

three-groups: second group dividing two halves

two-groups: group and non-group joins

two-groups-indexed: two groups with condition on one

two-groups-not-covering: join condition makes outer index not covering

no-stats: no statistics

no-stats-group: no statistics and group joins

no-stats-large: no statistics but reasonable row count

empty-stats: analyzed with no rows

scaled-distinct: mostly distinct now larger

scaled-not-distinct: not so distinct now larger

left-outer-index-usage: Outer join with a later condition that could be incorrectly serviced by index scan (bug980957)
