-- bit_or is an aggregate method call, which means expressions can be flattened in (unlike COUNT)
SELECT bit_or(x*4) AS b FROM (SELECT id+5 AS x from t1) AS anon1