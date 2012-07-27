bloom-filter-1: Use a Bloom filter to help with selective semi-join

bloom-filter-1l: LIMIT adjusted when costing

bloom-filter-1n: Extra columns means not semi-join

bloom-filter-1o: Outer join

bloom-filter-1x: Written with EXISTS

bloom-filter-2: Do not use when not selective

choose-condition-1: narrow and wide

choose-condition-2: wide and narrow

covering-or-group-scan: covering index or group scan?

distinct-sorted: DISTINCT + ORDER BY from index.

duplicate-condition: More than one comparison on same field

group-equals: two equals on group index

in-index: IN that can use index

in-sorted: IN that needs sorting

equals-sorted: Same with = for comparison.

equivalence-1: Equivalence column used in index.

in-subquery: IN SELECT to semi-join (not indexable).

in-subquery-indexed: IN that is indexed.

join-across-subquery: group join in subquery expression

join-cond-too-complex: A join condition that will not work with a group join.

join-cond-subquery: A join condition using a subquery expression.

right-too-complex-1: RIGHT join condition on child

right-too-complex-2: RIGHT join condition on parent

single-subquery: just derived table (should push down condition)

single-table: whole row from indexed table

sort-equals: ORDER BY an equality condition

subquery: join to subquery (with condition pushed down and combined into index)

three-groups: second group dividing two halves

two-groups: group and non-group joins

two-groups-indexed: two groups with condition on one

two-groups-not-covering: join condition makes outer index not covering

two-groups-aggregated: ordered index drives aggregate

no-stats: no statistics

no-stats-group: no statistics and group joins

no-stats-large: no statistics but reasonable row count

empty-stats: analyzed with no rows

scaled-distinct: mostly distinct now larger

scaled-not-distinct: not so distinct now larger

left-outer-index-usage: Outer join with a later condition that could be incorrectly serviced by index scan (bug980957)

outer-index: LEFT using an index

outer-no-index-extra-conditions: LEFT with extra conditions cannot use index

right-outer-index: RIGHT using an index

left-as-inner: LEFT index used for INNER join

right-as-inner: RIGHT index used for INNER join

cross-product: Conditions but no joins

competing-indexes-no-limit: WHERE beats ORDER BY

competing-indexes-with-limit: ORDER BY beats WHERE

group-scan-no-limit: group scan and flatten

group-scan-large-limit: with limit larger than expected

group-scan-small-limit: with much smaller limit

duplicate-table-conditions: condition between two occurrences of same table
