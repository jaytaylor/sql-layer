-- Key assertion, * expands to:
-- d.departmentid, d.filler, d.departmentname, e.lastname
-- WHERE clause becomes department.departmentid = 1 AND d.filler = 3
-- TODO this test should have a nested bindingContext, still not exactly sure
-- how to do that, will come back to it later, but this way I won't forget the test
SELECT TODO FILL THIS OUT* FROM department JOIN employee USING(departmentid,filler)
 WHERE departmentid = 1 AND filler = 3