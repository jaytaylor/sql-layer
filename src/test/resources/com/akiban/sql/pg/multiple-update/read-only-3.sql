BEGIN;
SET TRANSACTION READ ONLY;
SELECT name FROM customers;
COMMIT;
UPDATE customers SET name = 'Allowed' WHERE cid = 1;
