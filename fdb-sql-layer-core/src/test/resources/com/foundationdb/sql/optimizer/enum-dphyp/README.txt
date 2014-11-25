clique-3: 3-way conditions with WHERE

cross-2: two tables no joins

cross-5: five tables with three hyperedges

inner-2: two tables

inner-3: three tables

inner-4: 2 inner joins with a comparison to constant on the second join conditons

inner-5: 2 inner joins with a comparison to constant on the first join conditions

inner-left: inner then left

inner-left-cross: inner then left, then cross product on the right hand side of the inner

left-3: two tables, both left joined

left-3a: second condition to same parent

left-3n: null tolerant predicate prevents reordering

left-cross: left join with a cross product on the right hand side

left-inner: left then inner

left-inner-x: left then inner with condition to common parent

left-incomplete: left join with no condition

left-left-weird-conditons: two left joins with inner left join having conditons only involving outer left join

left-right-conditions: like left-left-weird-conditions, but the second join is a right join

left-single-table-condition: a left join with a condition just touching on the outer table

right: dphyp does not support right joins, they're supposed to be converted to left joins earlier on

semi-1: semi-join

semi-1a: inner join where exists

semi-2: semj-join to multiple inner joins

semi-2w: semj-join to multiple inner joins expressed as subquery WHERE

semi-3: where exists with an inner join on the inside that has all conditions on the where clause

semi-4: like semi-3, but the conditions for the inner join are in the ON clause

semi-5: inner join where exists with a comparison to a constant on inside the EXISTS clause

semi-6: cross join with a condition that turns into an inner join on the WHERE clause inside the WHERE EXISTS clause

semi-left-left: a semi join with a double left join on the left hand side