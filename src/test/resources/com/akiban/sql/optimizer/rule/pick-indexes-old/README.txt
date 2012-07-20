aggregate: index supplied order needed for aggregation

aggregate-no-sort: no index capable of ordering for GROUP BY

aggregate-sorted: ORDER BY and GROUP BY compatible

group-covering: covering group index

group-equals: two equals on group index

group-sort: group index for ordering and condition

no-index: no conditions or ordering

inequality-join: join only done by indexable inequality (note lack of any index)

order-covering: covering from ordering index

order-only: index for ordering with another condition

pkey-over-group: child looked up by index not by group operation

single-table: whole row from indexed table

sort-partial: sort by subset of columns

two-groups: group and non-group joins

two-groups-indexed: two groups with condition on one

left-outer-index-usage: Outer join with a later condition that could be incorrectly serviced by index scan (bug980957)

collating-index: collation cannot be covering
