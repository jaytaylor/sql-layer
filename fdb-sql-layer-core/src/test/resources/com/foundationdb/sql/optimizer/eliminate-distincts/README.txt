select-1: single unique column

select-2: single non-unique column

select-3: parent column that is unique and child column that is unique per parent

select-3i: same expressed with explicit joins

select-4: same with left join (conservatively not optimized, but could be)

select-5: outer join column

select-6: unique column but in outer join (might be multiple orphans)

select-7: regular column

select-8: regular column with primary key constraint

select-9: non-equality condition

select-10: no table projects
