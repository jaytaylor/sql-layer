after-ancestors: Select right after ancestor lookup, preventing one of the flattens.
                 (Better would be to do flattens in other order in
                  this case and / or cascading the ancestor lookups.)

between-lookups: After branch lookup and before ancestor lookup.
                 (Which it cannot prevent in this case.)

complex-conds: Complex conditions.

no-change: Multi-table conditions unmolested.

use-index: Test performed against index table itself before lookups.

group-index: Test against a group index.

cross-group: Push through cross-group join.

in: Push through semi-join.

multiple-table: Immediately after Flatten.

multiple-table-2: Product conditions do not move currently because branch not accessible.

multiple-table-3: Just indexes.

multiple-table-in: Can be before the semi-join.

collating-index: An index with with a non-recoverable key.

outer-join: Only required tables of outer join Flatten.
