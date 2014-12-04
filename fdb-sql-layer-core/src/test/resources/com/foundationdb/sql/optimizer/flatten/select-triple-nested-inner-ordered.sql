SELECT mid + 6 FROM (SELECT iid * 5 FROM (SELECT id-10 AS iid FROM parent WHERE id > 3 ORDER BY name) AS innerSelect WHERE iid < 5) AS midselect WHERE mid % 2 = 0
