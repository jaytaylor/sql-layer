-- Key assertion,
-- inner select clause binds departmentid to department.departmentid
select * from department JOIN employee USING(departmentid)
WHERE departmentid = (SELECT z FROM t1 WHERE t1.x = departmentid)