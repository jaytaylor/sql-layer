SELECT name FROM customers
 WHERE (cid,name) IN ((2,'Smith'),(1+1,'S'||'mith'),(1*2,'Smit'||'h'))
