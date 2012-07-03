duplicate-child: two occurrences of the same child table

duplicate-parent: join between two parent-child joins

in: IN condition

inner-beneath-left: INNER group join beneath LEFT join still gets recognized

join-across-subquery: group join in subquery expression

not-equals: Conditions other than equals

right-as-left: RIGHT join expressed as LEFT

right-as-right: RIGHT join expressed with RIGHT

right-from-list: RIGHT join with FROM list and WHERE

simple-from: group joined tables in FROM list

simple-outer: grouping outer joins

single-table: rows from single table only

subquery: group join across subquery boundary

two-groups: join between two groups

two-groups-left: LEFT join between two groups (stays LEFT)

two-groups-left-2: LEFT join between two groups other way (stays LEFT)

two-groups-right: RIGHT join between two groups (turns into LEFT)

group-via-equivalence-on-(left|inner|right): group joins where the grouping ON clause only works because of an
    equivalence. Note that the LEFT variant does *not* match grouping, due to the relatively complex
    condition involving OR NOT NULL.

group-via-equivalence-where-inner: group joins where the grouping WHERE clause only works because of an equivalence

group-via-equivalence-duplicates-conditions: Regression test for bug 947264. Equivalences cause a join condition to
    inappropriately turn into the condition of another join.