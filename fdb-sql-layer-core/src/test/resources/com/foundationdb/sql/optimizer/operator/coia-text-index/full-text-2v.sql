SELECT * FROM customers
   INNER JOIN orders USING (cid)
   INNER JOIN items USING (oid)
   INNER JOIN addresses USING (cid)
  WHERE FULL_TEXT_SEARCH(state = ? AND sku = ?)
