duplicate-child: same child table twice

index-value-columns: Condition on hkey column.

multiindex-single-branch: Intersection on single branch.

multiindex-multi-branch: Intersection on two branches.

multiindex-multi-duplicate: Intersection of two of the same index.

multiindex-multi-ordered: With ORDER BY.

single-table-1: covering index

single-table-2: group scan required

subquery-join-1: join to subquery (with condition pushed down)

subquery-join-2: WHERE pushed down for group index

subquery-join-3: subquery with its own joining using Hash Table for one join

three-groups: second group dividing two halves

multiple-table-conds-1: Conditions moved across maps.

multiple-table-conds-2: Same with indexes.

subquery-semi-join-limit: semi-join to subquery with limit
