insert into customers (cid, name)
select next value for test.customer_1, name
from  test.customers