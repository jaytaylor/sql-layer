SELECT c1.name AS n1, c2.name AS n2 FROM customers c1, customers c2
 WHERE c1.name < 'M' AND c2.name > c1.name
