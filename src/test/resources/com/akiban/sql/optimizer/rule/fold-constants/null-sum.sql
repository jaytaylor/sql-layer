SELECT SUM(SUBSTR(price,quan,NULL)), COUNT(sku) FROM items
 WHERE sku = '321'
