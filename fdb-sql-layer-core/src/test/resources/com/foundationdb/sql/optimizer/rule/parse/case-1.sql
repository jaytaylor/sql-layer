SELECT CASE WHEN sku = '1234' THEN 666 
            WHEN quan BETWEEN 10 AND 100 THEN 777 
            ELSE -1 END
  FROM items
