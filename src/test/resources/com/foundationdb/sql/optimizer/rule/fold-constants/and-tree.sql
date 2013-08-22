SELECT cid FROM customers
 WHERE (name IS NULL) OR (name <> 'Smith' AND name <> 'Jones' AND name <> 'Adams')