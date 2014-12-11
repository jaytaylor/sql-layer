-- there is a bit-or on the subquery, but not the part being referenced, so this one can be flattened
SELECT bit_or(x) AS b FROM (SELECT bit_or(id) as ignored, id+5 AS x from t1) AS anon1