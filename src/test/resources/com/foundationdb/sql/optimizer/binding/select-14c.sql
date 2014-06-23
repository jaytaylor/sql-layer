-- Key assertion, * expands to (note e.departmentid):
-- e.departmentid, d.filler, d.departmentname, e.lastname, e.filler
-- JOIN stays a RIGHT OUTER JOIN
SELECT * FROM department RIGHT OUTER JOIN employee USING(departmentid)