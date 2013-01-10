SELECT c1.cid FROM customers c1 
        INNER JOIN customers c2 ON c1.name = c2.name 
        INNER JOIN customers c5 ON c2.name = c5.name
        RIGHT JOIN customers c3 ON c2.name = c3.name
 WHERE EXISTS (SELECT * FROM customers c4 WHERE c4.name = c5.name)