count-product: Count rows from branch product, so no actual columns output.

count-outer: Count rows from outer join.

descendant: Only get columns from a descendant of index.

group-covering: A covering group index. No lookups.

group-equals: Group index and ancestor lookup from there.

group-sort: Group index for sorting and ancestor lookup from there.

multiindex-single-branch: Intersection on single branch.

multiindex-multi-branch: Intersection on two branches.

multiindex-multi-duplicate: Intersection of two of the same index.

multiindex-multi-ordered: With ORDER BY.

no-index: Group scan and flatten.

no-index-branch: Group scan and flatten along one branch and then
                 branch lookup the other branch a second time.

no-index-everything: A whole bunch of branches from a single group scan.

no-index-outer: Group scan and flatten with LEFT join.

order-covering: Index for ordering also covering. No lookups.

order-only: Index for ordering only.

right-inner: RIGHT expressed as LEFT

right-right: RIGHT join down.

side-branch: Index on one branch and all results from another branch,
             including an ancestor of the index.

side-branch-only: Only need other branch, no ancestors. 
                  Group index ensures parent exists.

side-branch-sorted: Side branch with sorting.

single-table: Single ancestor of index.

two-branches: Some fields on index ancestors and some from another branch.

two-branches-complex-1: Two branches with complex join condition.

two-branches-complex-2: Similar condition but on the other branch.

two-same-branches-1: Two branches with the same child table; output parent.

two-same-branches-1a: Output one child.

two-same-branches-1b: Output other child.

two-same-branches-2: Output parent and one child.

two-same-branches-2b: Output parent and other child.

two-same-branches-3: Output parent and both children.

two-side-branches: Two branches beside the condition one.

group-loop-unreferenced-1: Only table in group not used in group loop.

group-loop-unreferenced-2: Initial table in group not used in group loop.
