anti-with-onesided-condition1: anti join where the anti join has a condition that only touches the left hand side.

anti-with-onesided-condition2: similar to #1, but with a join on the right hand side (#1 creates a hash join)

anti-with-onesided-condition3: similar to #1, but the condition only references the inside

bloom-filter-1: Use a Bloom filter to help with selective semi-join

bloom-filter-1l: LIMIT adjusted when costing

bloom-filter-1n: Extra columns means not semi-join

bloom-filter-1o: Left outer join - should not create a bloom filter

bloom-filter-1x: Written with EXISTS

bloom-filter-2: Do not use when not selective

choose-condition-1: narrow and wide

choose-condition-2: wide and narrow

covering-or-group-scan: covering index or group scan?

distinct-sorted: DISTINCT + ORDER BY from index.

duplicate-condition: More than one comparison on same field

group-equals: two equals on group index

in-index: IN that can use index

in-index-no-union: IN that can use index

in-sorted: IN that needs sorting

in-sorted-no-union: IN that needs sorting

in-leftover: IN condition after more important joins

in-many: IN with lots of values

in-popular: IN with just popular values

equals-sorted: Same with = for comparison.

equivalence-1: Equivalence column used in index.

equivalence-2: Equivalence column used in GROUP BY.

exists-inner: semi join with inner join on right hand side

in-subquery: IN SELECT to semi-join (not indexable).

in-subquery-indexed: IN that is indexed.

in-subquery-distinct: IN with cross-group subquery with DISTINCT

inner-right-join: (INNER) RIGHT where the inner ON clause has a comparison with constant

inner-right-join2: (INNER) RIGHT where the inner ON clause has a comparison with constant without indexes

inner-left-join: (INNER) LEFT where the inner ON clause has a comparison with constant

join-across-subquery: group join in subquery expression

join-cond-too-complex: A join condition that will not work with a group join.

join-cond-subquery: A join condition using a subquery expression.

right-too-complex-1: RIGHT join condition on child

right-too-complex-2: RIGHT join condition on parent

single-subquery: just derived table (should push down condition)

single-table: whole row from indexed table

sort-equals: ORDER BY an equality condition

subquery: join to subquery (with condition pushed down and combined into index)

subquery-limit: subquery has limit

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

whole-group-nested: whole group as nested result set

group-branch-subquery: subquery from different branch (no group loop from flatten)

group-nested-sorted-1: outer scan sorted by index

group-nested-sorted-2: outer scan sorted by operator

geospatial-1: compute max radius for N neighbors

geospatial-2: get within that radius

geospatial-3: covering spatial index

geospatial-3n: same using modern function

geospatial-4: two spatial indexes considered for intersection

geospatial-5: basic overlapping

geospatial-6: constant point in indexed polygon

index-is-null: IS NULL condition

full-text-1: single parsed query

full-text-1l: with limit

full-text-1lp: with limit and other predicate

full-text-1v: from variable

full-text-2: terms on multiple branches

full-text-join: joined to another group

keys-1: primary key of root

keys-2: primary key of non-root

keys-2a: primary and foreign key of non-root

keys-3: all keys including those not present

keys-subquery: primary key from subselect

update-covering-1: Update needs whole row.

update-covering-2: Update with subquery on same table does not.

uncorrelated-semi-join: An EXISTS with no relation to the rest

uncorrelated-anti-join: NOT EXISTS with no relation to the rest

left-cross: LEFT join with cross join

hash-multiple-1: Multiple columns handled by same hash table

hash-multiple-2: Some conditions not handled by hash table

hash-left: Hash LEFT join

hash-semi: Hash SEMI join

m2m-order-by: many-to-many relation with multi-column order by
m2m-order-by-subquery: same as m2m-order-by, but part is in a subquery

order-by-subquery: order by in a subquery clause joined with another table
order-by-subquery2: same as order-by-subquery, but with an order-by on the outside too
order-by-subquery-provides-order-by: The subquery here is sorted the same as the order by, but the other table doesn't help

