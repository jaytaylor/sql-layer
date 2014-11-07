and-tree: Multiple ANDs flattened into Select.

case-1: CASE condition already worked out.

case-2: CASE only partially worked out.

case-3: CASE complex condition

counts: COUNT(NOT NULL column) turns into COUNT(*) but not NULL.

current-date-time: Current date / time is not a constant.

extra-true: true conditions are ignored in WHERE clause.

failing-condition: false conditions kill the whole SELECT.

failing-inner-join: false inner join conditions kill the whole SELECT as well.

failing-outer-join: false outer join conditions only kill the optional side.

having-1: HAVING condition unmolested.

having-2: MAX precomputed if empty.

having-3: If aggregation known, HAVING conditions can then be tested ahead of time.

impossible-in: Condition built from IN cannot possibly be true.
               Still might match zero rows, but that's false too.

impossible-is-null: A NOT NULL column cannot satisfy IS NULL.

possible-is-null: A NOT NULL column on an outer join can.

if-1: IF() special null handling.

if-2: IF() with constant.

in-duplicates: IN list with duplicate literals.

in-expressions: IN list with expressions.

in-literals: IN list with just literals.

in-non-top-level: IN list inside complex condition.

in-nulls: IN list with nulls.

in-singleton: IN list with single value after folding.

in-singleton-row: IN list with single row value.

in-constant: IN with some constants

in-constant-row: IN row value with some constants

in-constant-equals: IN with some constants that are equal

is-conditions: IS TRUE / FALSE / NULL.

logical-functions: NOT TRUE is FALSE, TRUE AND X is X, FALSE AND X is FALSE, 
                   X OR FALSE is X.

no-change: basic query unmolested.

null-aggregate: Aggregation over empty source (with impossible condition) NULL or 0.

null-condition: NULL to ordinary function is NULL, compare with NULL is unknown.

null-in-select: NULL IN will be unknown for all conditions, so IN is
                unknown unless empty, in which case it's false. Which
                won't satisfy either way.

null-or-false: unknown AND X is either unknown or false, 
               (unknown or false) OR (unknown or false) is never true,
               condition not satisfied.

null-semi-join: empty EXISTS is false.

null-subaggregate: SELECT for value with aggregation from empty is
                   precomputed and moved up into outer query.

null-subaggregate-in: SELECT for value with aggregation from empty as part of ANY
                      computed condition result.

null-subquery: outer join to empty subselect adds NULLs.

null-subselect: SELECT for value from empty is NULL.

null-sum: SUM(NULL) is NULL.

select-select-null: SELECT NULL for value is NULL if empty, NULL if not.
                    NULL either way.

select-null: SELECT NULL;

unnecessary-all: ALL of condition that must be true is also true if empty. 
                 true either way.

nested-result-set-exprs: Expressions in nested result set
