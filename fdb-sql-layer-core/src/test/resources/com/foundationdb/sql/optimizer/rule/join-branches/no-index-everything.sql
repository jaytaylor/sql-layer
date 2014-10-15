SELECT name, state, order_date, sku, ship_date, referral
  FROM customers 
 INNER JOIN addresses ON customers.cid = addresses.cid
 INNER JOIN orders ON customers.cid = orders.cid
 INNER JOIN items ON orders.oid = items.oid
 INNER JOIN shipments ON orders.oid = shipments.oid
 INNER JOIN referrals ON customers.cid = referrals.cid
