SELECT c1.cid FROM customers c1 
        LEFT JOIN customers c2 ON c1.name = c2.name
        RIGHT JOIN customers c3 on c2.name = c3.name
 WHERE EXISTS (SELECT * FROM customers c4 WHERE c4.name = c1.name)