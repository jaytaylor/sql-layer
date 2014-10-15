-- Key assertion, * expands to:
-- d.departmentid, d.filler, d.departmentname, e.lastname
-- WHERE clause becomes department.departmentid = 1 AND d.filler = 3
SELECT * FROM department JOIN employee USING(departmentid,filler)
 WHERE departmentid = 1 AND filler = 3