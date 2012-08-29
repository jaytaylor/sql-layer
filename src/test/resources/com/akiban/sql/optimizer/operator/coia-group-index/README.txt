COIA with two group indexes

count-1: count group index result

select-1: columns from same branch as group index

select-2: group index for matching and ordering

select-2bt: BETWEEN condition

select-2eq: = condition

select-2ge: >= condition

select-2le: <= condition

select-2lt: < condition

select-2r: reverse ordering

select-3: group index for ordering only

select-3l: group index for ordering only with left join

select-3m: group index in mixed mode

select-4: covering group index

select-5: index IS NULL

select-7l: literal in result list

select-7o: ordering group index

select-7oo: ordering group index with left join at leaf

select-7ooo: ordering group index with left joins along

select-12: various multi-column index uses

select-13: IN using index union

select-14: IN using index

select-14p: IN using index with parameters

select-15: IN SELECT using group index in inner loop

select-15d: IN SELECT using group index in outer loop (via DISTINCT)

select-16: DISTINCT

select-16s: DISTINCT with ORDER BY using Sort

select-16l: DISTINCT with ORDER BY using Sort with LIMIT

select-16g: DISTINCT with ORDER BY using group index

select-17a: side branch with sort

select-17b: side branch with sort and IN

select-18: range inequalities

select-18n: range inequalities and IS NULL

select-19i: aggregation and IN

select-19x: aggregation from MAX

select-20: subquery with cross-group join
