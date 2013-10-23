select cid, name from customers
UNION
select cat, sku from categories
order by 1