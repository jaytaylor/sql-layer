-- Key assertion, * expands to (note e.departmentid):
-- employee.departmentid, d.filler, d.departmentname, e.lastname, e.filler
-- WHERE clause becomes employee.departmentid = 1
-- NOTE: it won't set the tableName on the whereClause.leftOperand, just the userData
SELECT * FROM department RIGHT OUTER JOIN employee USING(departmentid) WHERE departmentid = 1