-- Key assertion, * expands to:
-- desk.departmentid, d.filler, d.departmentname, e.lastname, e.filler, desk.title, desk.filler
-- And both USING clauses/conditions are correct
SELECT * FROM department LEFT OUTER JOIN employee USING(departmentid) RIGHT OUTER JOIN desk USING(departmentid)