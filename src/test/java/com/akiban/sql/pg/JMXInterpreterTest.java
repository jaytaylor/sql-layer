/**
 * Copyright (C) 2012 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.pg;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.akiban.server.store.statistics.IndexStatisticsMXBean;

/* test class for JMXIterpreter, which provides a JMX interface to the server in the test framework  */
public class JMXInterpreterTest {

    private static final String SERVER_JMX_PORT = "8082";
    private static final String SERVER_ADDRESS = "localhost";

    @Test
    public void testForBasicConstructor() {
        JMXInterpreter conn = new JMXInterpreter();
        conn.openConnection(SERVER_ADDRESS, SERVER_JMX_PORT);
        conn.getInfo(conn.setup(conn.getConnector()));
        conn.close();
    }

    @Test
    public void testCallToManageMXbean() {
        JMXInterpreter conn = new JMXInterpreter();
        conn.openConnection(SERVER_ADDRESS, SERVER_JMX_PORT);
        Assert.assertNotNull(conn.getManageMXBean(conn.getConnector()));
        System.out.println(conn.getManageMXBean(conn.getConnector())
                .getVersionString());

        conn.close();
    }

    @Test
    public void testCalltoIndexStatisticsMXBean() {
        JMXInterpreter conn = new JMXInterpreter();
        conn.openConnection(SERVER_ADDRESS, SERVER_JMX_PORT);
        IndexStatisticsMXBean bean = conn.getIndexStatisticsMXBean(conn
                .getConnector());
        Assert.assertNotNull(bean);
        try {
            bean.dumpIndexStatistics("test", "/tmp/test.dmp");
        } catch (IOException e) {
            System.out.println(e.getMessage());
            Assert.fail(e.getMessage());
        }

        conn.close();
    }

}
