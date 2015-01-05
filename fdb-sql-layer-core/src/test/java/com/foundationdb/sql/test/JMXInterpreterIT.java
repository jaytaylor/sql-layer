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

package com.foundationdb.sql.test;

import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.jmx.JmxManageable;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class JMXInterpreterIT extends ITBase
{
    private static final int SERVER_JMX_PORT = 8082;
    private static final String SERVER_ADDRESS = "localhost";
    private static final String TYPE_NAME = "JMXTest";
    private static final String BEAN_NAME = "com.foundationdb:type="+TYPE_NAME;

    @SuppressWarnings("unused")
    public interface JMXTestMXBean {
        public int getIntValue();
        public String getStringValue(String s);
    }

    public interface JMXTestService {
    }

    public static class JMXTestServiceImpl implements Service, JmxManageable, JMXTestService, JMXTestMXBean
    {
        @Override
        public JmxObjectInfo getJmxObjectInfo() {
            return new JmxObjectInfo(TYPE_NAME, this, JMXTestMXBean.class);
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public void crash() {
        }

        @Override
        public int getIntValue() {
            return 42;
        }

        @Override
        public String getStringValue(String s) {
            return "A string: " + s;
        }
    }


    @Override
    protected Map<String,String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                    .bindAndRequire(JMXTestService.class, JMXTestServiceImpl.class);
    }

    @Test
    public void testForBasicConstructor() throws Exception {
        try(JMXInterpreter conn = new JMXInterpreter()) {
            conn.ensureConnection(SERVER_ADDRESS, SERVER_JMX_PORT);
            assertNotNull(conn);
            MBeanServerConnection mbs = conn.getAdapter().getConnection();
            ObjectName bean = new ObjectName(BEAN_NAME);
            assertNotNull("bean", bean);
            MBeanInfo info = mbs.getMBeanInfo(bean);
            assertEquals("attr count", 1, info.getAttributes().length);
            assertEquals("ops count", 1, info.getOperations().length);
        }
    }

    @Test
    public void testCall() throws Exception {
        try(JMXInterpreter conn = new JMXInterpreter()) {
            Object[] parameters = { "test" };
            Object data = conn.makeBeanCall(
                SERVER_ADDRESS,
                SERVER_JMX_PORT,
                BEAN_NAME,
                "getStringValue",
                parameters,
                "method"
            );
            assertEquals("A string: test", data);
        }
    }
}
