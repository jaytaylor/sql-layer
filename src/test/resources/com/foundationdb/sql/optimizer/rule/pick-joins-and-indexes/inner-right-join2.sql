-- This is a redo of an issue with Sequel schema parsing (which uses information_schema tables)
SELECT items.sku, categories.cat
FROM categories
INNER JOIN sources
  ON (sources.country = 'USA')
  AND (sources.sku = categories.sku)
RIGHT OUTER JOIN items
  ON (items.sku = categories.sku)
WHERE items.quan = 12
  AND items.oid = 5