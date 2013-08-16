CREATE TABLE t(
    id INT NOT NULL PRIMARY KEY,
    name VARCHAR(32)
);

CREATE PROCEDURE proc(IN x INT) LANGUAGE javascript PARAMETER STYLE variables DYNAMIC RESULT SETS 1 AS $$
  java.sql.DriverManager
  .getConnection("jdbc:default:connection")
  .createStatement()
  .executeQuery("SELECT " + x + " * id AS mul FROM test.t");
$$;