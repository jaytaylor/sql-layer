/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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
import javax.management.ObjectName;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.Assert.assertNotNull;

public class JMXInterpreterIT extends PostgresServerYamlITBase
{
    private static final Logger LOG = LoggerFactory.getLogger(JMXInterpreterIT.class);

    private static final int SERVER_JMX_PORT = 8082;
    private static final String SERVER_ADDRESS = "localhost";

    @Test
    public void testForBasicConstructor() throws Exception {
        try(JMXInterpreter conn = new JMXInterpreter()) {
            conn.ensureConnection(SERVER_ADDRESS, SERVER_JMX_PORT);
            LOG.debug("serviceURL: " + conn.serviceURL);
            assertNotNull(conn);

            final MBeanServerConnection mbs = conn.getAdapter().getConnection();
            ObjectName mxbeanName = new ObjectName("com.foundationdb:type=IndexStatistics");
            MBeanInfo info = mbs.getMBeanInfo(mxbeanName);
            MBeanOperationInfo[] ops = info.getOperations();
            for(int x = 0; x < ops.length; x++) {
                MBeanOperationInfo op = ops[x];
                LOG.debug("{} Return type({}): {}", new Object[]{x, op.getDescription(), op.getReturnType()});
                for(int a = 0; a < ops[x].getSignature().length; a++) {
                    LOG.debug("  {}: {}", op.getSignature()[a].getDescription(), op.getSignature()[a].getType());
                }
            }
        }
    }

    @Test
    public void testCall() throws Exception {
        try(JMXInterpreter conn = new JMXInterpreter()) {
            Object[] parameters = { "test" };
            Object data = conn.makeBeanCall(
                SERVER_ADDRESS,
                SERVER_JMX_PORT,
                "com.foundationdb:type=IndexStatistics",
                "dumpIndexStatisticsToString",
                parameters,
                "method"
            );
            LOG.debug("data: {}", data);
        }
    }
}
