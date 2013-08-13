SELECT CASE sku
       WHEN '1234' THEN 'apples'
       WHEN '9876' THEN 'oranges'
       ELSE 'unknown' END
  FROM items
