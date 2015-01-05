aggregate-1: call basic aggregate functions

aggregate-2: COUNT(*)

aggregate-2s: Should get same ORDER BY for free.

aggregate-2n: Need sort for different ORDER BY.

aggregate-3: no GROUP BY

cast-1: implicit casts

cast-2: explicit casts, including scale adjustment

delete-1: single column condition

insert-1: multiple VALUES 

insert-2: from SELECT

select-1: whole row from single table

select-2: group index conditions

select-3: index one branch, LEFT JOIN to another

select-5: index one branch, join in another

select-6: index intersection with different column counts

select-7: index scan with empty range (contradictory conditions)

update-1: set to literal

geospatial-1: compute max radius for N neighbors

geospatial-2: get within that radius

geospatial-3: covering spatial index

geospatial-5: Very large radius, testing handling of query regions exceeding latitude and longitude bounds.

geospatial-6: non-point index

full-text-1: single parsed query

full-text-2: terms on multiple branches
