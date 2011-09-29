after-ancestors: Select right after ancestor lookup, preventing one of the flattens.
                 (Better would be to do flattens in other order in
                  this case and / or cascading the ancestor lookups.)

between-lookups: After branch lookup and before ancestor lookup.
                 (Which it cannot prevent in this case.)

no-change: Conditions unmolested.

use-index: Test performed against index table itself before lookups.
