-- Key assertion,
-- inner select clause binds departmentid to employee.departmentid
select * from department RIGHT OUTER JOIN employee USING(departmentid)
WHERE departmentid = (SELECT z FROM t1 WHERE t1.x = departmentid)