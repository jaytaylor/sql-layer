select * from (
select cid as id, name as text  from customers
UNION
select cat as id, sku as text from categories) AS new_table
where id = 1