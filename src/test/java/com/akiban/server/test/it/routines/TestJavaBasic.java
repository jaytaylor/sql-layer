
package com.akiban.server.test.it.routines;

/** Basic Java stored procedures
 * <code><pre>
CALL sqlj.install_jar('target/akiban-server-1.4.3-SNAPSHOT-tests.jar', 'testjar', 0);
CREATE PROCEDURE test.add_sub(IN x INT, IN y INT, OUT "sum" INT, out diff INT) LANGUAGE java PARAMETER STYLE java EXTERNAL NAME 'testjar:com.akiban.server.test.it.routines.TestJavaBasic.addSub';
CALL test.add_sub(100,59);
 * </pre></code> 
 */
public class TestJavaBasic
{
    public static void addSub(int x, int y, int[] sum, int[] diff) {
        sum[0] = x + y;
        diff[0] = x - y;
    }
}
