-- Key assertion, * expands to:
-- d.departmentid, d.filler, d.departmentname, e.lastname, e.filler
SELECT * FROM department JOIN employee USING(departmentid)