COIA with two group indexes

count-1: count group index result

select-1: columns from same branch as group index

select-2: group index for matching and ordering

select-3: group index for ordering only

select-3l: group index for ordering only with left join

select-4: covering group index

select-7l: literal in result list

select-7o: ordering group index

select-7oo: ordering group index with left join at leaf

select-7ooo: ordering group index with left joins along

select-14: IN using index

select-14p: IN using index with parameters

select-15: IN SELECT using group index in inner loop

select-15d: IN SELECT using group index in outer loop (via DISTINCT)

select-16: DISTINCT

select-16s: DISTINCT with ORDER BY using Sort

select-16g: DISTINCT with ORDER BY using group index

select-17a: side branch with sort

select-17b: side branch with sort and IN
