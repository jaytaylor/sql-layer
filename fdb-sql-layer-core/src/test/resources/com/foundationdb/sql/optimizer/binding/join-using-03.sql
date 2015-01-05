-- Key assertion, * expands to:
-- d.departmentid, d.filler, d.departmentname, e.lastname, e.filler
-- Join stays LEFT JOIN
SELECT * FROM department LEFT OUTER JOIN employee USING(departmentid)