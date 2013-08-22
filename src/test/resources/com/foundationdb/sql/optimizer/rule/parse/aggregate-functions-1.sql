SELECT max(price), min(price), sum(quan), avg(quan),
 count(oid), count(*), count(distinct oid)
FROM items
