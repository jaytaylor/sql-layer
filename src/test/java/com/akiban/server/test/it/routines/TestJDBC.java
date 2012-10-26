/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
