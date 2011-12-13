count-product: Count rows from branch product, so no actual columns output.

descendant: Only get columns from a descendant of index.

group-covering: A covering group index. No lookups.

group-equals: Group index and ancestor lookup from there.

group-sort: Group index for sorting and ancestor lookup from there.

join-cond-too-complex: A join condition that will not will not work with a group join. (Unsupported)

no-index: Group scan and flatten.

no-index-branch: Group scan and flatten along one branch and then
                 branch lookup the other branch a second time.

no-index-everything: A whole bunch of branches from a single group scan.

no-index-outer: Group scan and flatten with LEFT join.

order-covering: Index for ordering also covering. No lookups.

order-only: Index for ordering only.

right-inner: RIGHT expressed as LEFT

right-right: RIGHT join down.

right-too-complex: RIGHT join condition. (Unsupported)

side-branch: Index on one branch and all results from another branch,
             including an ancestor of the index.

side-branch-sorted: Side branch with sorting.

single-table: Single ancestor of index.

two-branches: Some fields on index ancestors and some from another branch.

two-branches-complex-1: Two branches with complex join condition.

two-branches-complex-2: Similar condition but on the other branch.

