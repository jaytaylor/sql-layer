
package com.akiban.server.test.it.routines;

import java.sql.*;

/** Java stored procedures using JDBC
 * <code><pre>
DROP TABLE test2;
CREATE TABLE test2(n INT NOT NULL, value VARCHAR(10));
INSERT INTO test2 VALUES(1, 'aaa'), (2, 'bbb'), (1, 'xyz');
CALL sqlj.install_jar('target/akiban-server-1.4.3-SNAPSHOT-tests.jar', 'testjar', 0);
CREATE PROCEDURE test.split_results(IN n INT) LANGUAGE java PARAMETER STYLE java RESULT SETS 2 EXTERNAL NAME 'testjar:com.akiban.server.test.it.routines.TestJDBC.splitResults';
CALL test.split_results(1);
 * </pre></code> 
 */
public class TestJDBC
{
    public static void splitResults(int n, ResultSet[] rs1, ResultSet[] rs2) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:default:connection", "test", "");
        PreparedStatement ps1 = conn.prepareStatement("SELECT * FROM test2 WHERE n = ?");
        PreparedStatement ps2 = conn.prepareStatement("SELECT * FROM test2 WHERE n <> ?");
        ps1.setInt(1, n);
        ps2.setInt(1, n);
        rs1[0] = ps1.executeQuery();
        rs2[0] = ps2.executeQuery();
    }
}
