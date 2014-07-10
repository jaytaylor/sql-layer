-- At the time of writing this, FULL OUTER JOIN doesn't work, but
-- with strange exceptions, in an effort to not have to implement
-- the challenging FULL OUTER JOIN USING, this is becoming explicit
-- if we start supporting FULL OUTER JOIN we should deal with this.
SELECT * FROM department FULL OUTER JOIN employee USING(departmentid)