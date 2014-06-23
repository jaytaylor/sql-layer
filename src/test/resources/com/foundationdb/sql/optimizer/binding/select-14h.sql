-- Key assertion, * expands to:
-- employee.departmentid, d.filler, d.departmentname, e.lastname, e.filler, desk.title, desk.filler
-- And second USING compares employee.departmentid with desk.departmentid
SELECT * FROM department RIGHT OUTER JOIN employee USING(departmentid) LEFT OUTER JOIN desk USING(departmentid)