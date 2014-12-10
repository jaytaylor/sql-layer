-- bit_or is a normal function, count is a special function, both should prevent folding
SELECT bit_or(x) AS b FROM (SELECT COUNT(*) AS x from t1) AS anon1