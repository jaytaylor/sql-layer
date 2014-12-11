-- bit_or is a normal function, count is a special function, both should prevent folding
SELECT COUNT(*) AS c FROM (SELECT bit_or(c1) from t1) AS anon1