-- This should throw an ambiguous error, because the departmentid of desk is a
-- duplicate
SELECT * FROM department JOIN employee USING(departmentid)
 JOIN desk ON department.filler = desk.filler
 WHERE departmentid = 1