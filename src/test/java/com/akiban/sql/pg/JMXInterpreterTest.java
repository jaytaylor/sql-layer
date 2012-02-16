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

import javax.management.remote.JMXConnector;

import junit.framework.Assert;

import org.junit.Test;

import com.akiban.server.manage.ManageMXBean;
import com.akiban.server.store.statistics.IndexStatisticsMXBean;

/* test class for JMXIterpreter, which provides a JMX interface to the server in the test framework  */
public class JMXInterpreterTest {

    private static final String SERVER_JMX_PORT = "8082";
    private static final String SERVER_ADDRESS = "localhost";

    
    public void testForBasicConstructor() {
        JMXInterpreter conn = new JMXInterpreter();
        conn.openConnection(SERVER_ADDRESS, SERVER_JMX_PORT);
        conn.getInfo(conn.setup(conn.getConnector()));
        conn.close();
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
