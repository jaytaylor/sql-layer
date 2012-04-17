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

package com.akiban.sql.pg;

import java.io.IOException;

import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.akiban.sql.pg.JMXInterpreter.JmxAdapter;
import junit.framework.Assert;

import org.junit.Test;

/* test class for JMXIterpreter, which provides a JMX interface to the server in the test framework  */
public class JMXInterpreterIT extends PostgresServerYamlITBase {

    private static final int SERVER_JMX_PORT = 8082;
    private static final String SERVER_ADDRESS = "localhost";

    @Test
    public void testForBasicConstructor()  {
        JMXInterpreter conn = null;
        try {
            conn = new JMXInterpreter(true);
            conn.ensureConnection(SERVER_ADDRESS, SERVER_JMX_PORT);
            System.out.println(conn.serviceURL);
            Assert.assertNotNull(conn);
            
            final MBeanServerConnection mbs = conn.getAdapter().getConnection();
            MBeanInfo info = null;
            ObjectName mxbeanName = null;
            try {
                mxbeanName = new ObjectName(
                        "com.akiban:type=IndexStatistics");
            } catch (MalformedObjectNameException e1) {
                e1.printStackTrace();
                Assert.fail(e1.getMessage());
            } catch (NullPointerException e1) {
                e1.printStackTrace();
                Assert.fail(e1.getMessage());   
            }
            try {
                info = mbs.getMBeanInfo(mxbeanName);
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
            MBeanOperationInfo[] ops = info.getOperations();
            for (int x=0;x < ops.length;x++) {
                System.out.println(x+" Return type("+ops[x].getDescription()+"): "+ ops[x].getReturnType());
                for (int a=0;a < ops[x].getSignature().length;a++) {
                  System.out.println("  "+ops[x].getSignature()[a].getDescription()+":"+ops[x].getSignature()[a].getType());
                }
            }
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testCall() throws Exception  {
        JMXInterpreter conn = null;
        try {
            conn = new JMXInterpreter(true);
            Object[] parameters = { new String("test") };
            Object data = conn.makeBeanCall( 
                    SERVER_ADDRESS, 
                    SERVER_JMX_PORT, 
                    "com.akiban:type=IndexStatistics", 
                    "dumpIndexStatisticsToString", 
                    parameters, "method");
            System.out.println(""+data);
            
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    

}
