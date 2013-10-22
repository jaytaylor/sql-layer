COI with normal indexes

count-1: COUNT(*) root table

count-2: COUNT(*) with indexed condition

count-3: COUNT(*) with joins and index and non-index conditions

count-4: COUNT(*) from multiple tables

delete-1: DELETE by primary key

delete-2: DELETE by general condition

delete-3: DELETE RETURNING basic

delete-4: DELETE RETURNING with formula

delete-5: DELETE RETURING *

insert-1: INSERT single row VALUES

insert-2: INSERT multiple row VALUES

insert-3: INSERT with explicit columns

insert-4: INSERT from SELECT

insert-5: INSERT RETURNING basic

insert-6: INSERT RETURING with formula

select-0: single column from primary key match

select-1: whole row from root group scan

select-1l: whole row from leaf group scan

select-1n: select only ordering covering index field

select-2: leaf inequality condition and columns from branch

select-2a: leaf condition and columns from branch

select-2b: leaf range condition and columns from branch

select-2p: comparison operand from parameter

select-2r: tables named in reverse order

select-3: indexed and non-indexed condition

select-4: explicit joins rather than FROM list

select-5: order by descendant

select-5o: order by descendant with LEFT join (former fail)

select-6: index for ordering and other conditions

select-7l: only literals in result list

select-7o: must join to check against orphans

select-8: columns and literals

select-9: test for bug 838907

select-10: IN with order only index (cf. bug 876586)

select-11: aggregation in derived table

select-12: list of AND !='s into UNION

select-13: cross group outer join

select-14: NOT EXISTS

select-15: index intersection and union

select-15a: intersect two unions (one union all)

select-15i: with different sized integers

select-16: Use a Bloom filter to help with selective semi-join

select-17: Various index uses to show EXPLAIN variation

select-18: Really long IN shouldn't turn into UNIONs

union-1: simple union

union-2: union with order by

union-3: 3 way union

union-4: cast different types for union

union-5: cast different types for union all

union-6: select * union

union-7: Left column null 

union-8: both sides null column

union-9: constant expression right side of union

update-1: change column to literal

update-2: change column to expression (column itself)

update-3: update from (unnested) subquery

update-4: returning

update-5: returning with expressions

update-6: update with subquery

update-7: update with subquery on same table and indexable join
