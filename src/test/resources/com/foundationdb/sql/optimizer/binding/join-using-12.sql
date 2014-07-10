-- This just checks that you can still access the column from the table
SELECT employee.departmentid, employee.filler FROM department JOIN employee USING(departmentid)
 WHERE department.departmentid = 3