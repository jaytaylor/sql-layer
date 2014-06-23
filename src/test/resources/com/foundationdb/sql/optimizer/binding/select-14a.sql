-- Key assertion, * expands to:
-- d.departmentid, d.filler, d.departmentname, e.lastname, e.filler
-- WHERE clause becomes department.departmentid = 1
SELECT * FROM department JOIN employee USING(departmentid) WHERE departmentid = 1