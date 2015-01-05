SELECT sku, SUM(quan) AS quan FROM items
 GROUP BY sku
 ORDER BY quan DESC