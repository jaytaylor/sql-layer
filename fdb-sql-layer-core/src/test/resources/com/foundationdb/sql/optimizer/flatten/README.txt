combine-expressions: an expression on the outside and an expression on the subquery

bit-or-bit-or: bit_or in subquery ignored so outer bit_or ok
bit-or-count: bit_or on the outside, with count in the subquery
bit-or-expression-expression: bit_or with expression in the argument & subquery
bit-or-expression-joined: bit_or(expression sourcing two subqueries, one with aggregate)
bit-or-expression: bit_or with a bit_or in the subquery

count-aliased-column: count on outside of column that is aliased in the subquery
count-column-to-star: count on outside of a column, where subquery is SELECT *
count-column: count on outside that just references column returned in subquery
count-count: count of a subquery that is a count
count-expression: count of a column where the subquery has an expression on that column
count-in: counting a field, flatten IN clause
count-in-expression: counting a field, flatten IN clause
count-join-expression: count(*) on outside, two subqueries, one of which has an expression
count-star-expression: count(*) with an expression in the subquery

delete-1: just delete everything from a child -> causes a SelectNode with almost everything null
delete-in: a delete with an IN clause in the WHERE clause

expression-count: an expression on the outside, and a count on the only subquery

select-1: subquery with unique column

select-2: ANY instead of IN

select-3: ANY > instead of =

select-4: subquery inequality

select-5: subquery without unique column

select-6: subquery with multiple tables but still unique

select-6j: same with explicit INNER JOIN

select-6n: uniqueness from index and join condition

select-7: EXISTS with unique constraint

select-8: view

select-8o: view with ORDER BY

select-9: VALUES single row

select-9x: VALUES multiple rows (no change)

select-10: outer comparison instead of ANY

star-bit-or-join: bit_or on one side a joined subquery
star-count-join: outer query is just *, one subquery has a COUNT, the other does not
