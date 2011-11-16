SELECT name FROM customers RIGHT JOIN orders USING (cid)
WHERE (name != 'Arthur' AND name != 'John' AND name != 'Victor') OR name IS NULL