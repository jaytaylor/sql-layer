-- Key assertion, * expands to:
-- d.departmentid, d.filler, d.departmentname, e.lastname, e.filler
-- WHERE clause becomes department.departmentid = 1
-- NOTE: it won't set the tableName on the whereClause.leftOperand, just the userData
SELECT * FROM department JOIN employee USING(departmentid) WHERE departmentid = 1