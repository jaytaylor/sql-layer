/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.pg;

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
                        "com.foundationdb:type=IndexStatistics");
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
                    "com.foundationdb:type=IndexStatistics", 
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
