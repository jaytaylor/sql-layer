COI with no indexed other than primary keys

select-0; no FROM list

select-2: inequality

select-02a: match column

select-02b: match range

select-02p: compare with parameter

select-3: two conditions on same table

select-4: explicit joins

select-5: sorting (former fail)

select-6: sorting without limit

select-06l: sort with limit

select-06o: sort with offset

select-06ol: sort with offset and limit

select-07l: literal result column

select-07o: join only for orphans

select-9: extra true condition

select-10: aggregate with DISTINCT

select-12: two aggregates with DISTINCT on same column

select-13p: parameter types with and without sharing

union-1: basic union

union-2: forced group scan for union

union-3: UNION with NULLs

union-3s: UNION with NULLs as subquery

update-1: change column to literal

update-2: impossible condition
