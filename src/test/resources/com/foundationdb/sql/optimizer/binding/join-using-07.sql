-- Key assertion, * expands to:
-- d.departmentid, d.filler, d.departmentname, e.lastname, e.filler, desk.title, desk.filler
-- And both USING clauses/conditions are correct
SELECT * FROM department JOIN employee USING(departmentid) LEFT OUTER JOIN desk USING(departmentid)