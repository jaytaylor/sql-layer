-- Key assertion, * expands to (note e.departmentid):
-- e.departmentid, d.filler, d.departmentname, e.lastname, e.filler
-- WHERE clause becomes employee.departmentid = 1
SELECT * FROM department RIGHT OUTER JOIN employee USING(departmentid) WHERE departmentid = 1