-- Key assertion, * expands to:
-- d.departmentid, e.filler
SELECT departmentid, employee.filler FROM department JOIN employee USING(departmentid)