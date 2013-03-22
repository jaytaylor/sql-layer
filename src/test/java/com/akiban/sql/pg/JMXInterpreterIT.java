
package com.akiban.sql.pg;

import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

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
