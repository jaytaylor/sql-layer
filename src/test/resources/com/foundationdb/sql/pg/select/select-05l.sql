SELECT customers.name,order_date FROM customers,orders WHERE customers.cid = orders.cid ORDER BY order_date DESC LIMIT 2
