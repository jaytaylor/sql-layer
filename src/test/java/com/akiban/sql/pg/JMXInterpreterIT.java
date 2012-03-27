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

import javax.management.remote.JMXConnector;

import junit.framework.Assert;

import org.junit.Test;

import com.akiban.server.manage.ManageMXBean;
import com.akiban.server.store.statistics.IndexStatisticsMXBean;

/* test class for JMXIterpreter, which provides a JMX interface to the server in the test framework  */
public class JMXInterpreterIT extends PostgresServerYamlITBase {

    private static final String SERVER_JMX_PORT = "8082";
    private static final String SERVER_ADDRESS = "localhost";

    @Test
    public void testForBasicConstructor() {
        JMXInterpreter conn = null;
        try {
            conn = new JMXInterpreter();
            conn.openConnection(SERVER_ADDRESS, SERVER_JMX_PORT);
            Assert.assertNotNull(conn);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testCallToAkServer() {
        JMXInterpreter conn = new JMXInterpreter();
        try {
            conn.openConnection(SERVER_ADDRESS, SERVER_JMX_PORT);
            Assert.assertNotNull(conn);
            JMXConnector connector = conn.getConnector();
            Assert.assertNotNull(connector);
            ManageMXBean bean = conn.getAkServer(connector);

            Assert.assertNotNull(bean);
            Assert.assertNotNull(bean.getVersionString());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCalltoIndexStatisticsMXBean() throws IOException {
        JMXInterpreter conn = new JMXInterpreter();
        try {
            conn.openConnection(SERVER_ADDRESS, SERVER_JMX_PORT);
            IndexStatisticsMXBean bean = conn.getIndexStatisticsMXBean(conn
                    .getConnector());
            Assert.assertNotNull(bean);
            bean.dumpIndexStatistics("test", "/tmp/test.dmp");
        } finally {
            conn.close();
        }
    }

}
