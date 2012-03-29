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

package com.akiban.server.service.jmx;

import static junit.framework.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.Test;

public final class JmxRegistryServiceImplTest {
    private interface TestMXBean {
        public int getInt();
    }

    private interface NotMXBeanInterface {
        public int getInt();
    }

    private interface SubInterface extends TestMXBean {

    }

    private static class GoodService implements TestMXBean, JmxManageable {
        private final String name;

        private GoodService(String name) {
            this.name = name;
        }

        @Override
        public int getInt() {
            return 2;
        }

        @Override
        public JmxObjectInfo getJmxObjectInfo() {
            return new JmxObjectInfo(name, this, TestMXBean.class);
        }
    }

    private static class MockedJmxRegistry extends JmxRegistryServiceImpl {
        final MBeanServerProxy.MockMBeanServer mock = MBeanServerProxy.getMock();
        @Override
        protected MBeanServerProxy.MockMBeanServer getMBeanServer() {
            return mock;
        }
    }

    @Test(expected=JmxRegistrationException.class)
    public void managedClassNotAnInterface() {
        class MyMXBean implements JmxManageable {
            public int getInt() { return 1; }

            @Override
            public JmxObjectInfo getJmxObjectInfo() {
                return new JmxObjectInfo("Test", this, MyMXBean.class);
            }
        }

        MockedJmxRegistry service = new MockedJmxRegistry();
        final MyMXBean probe = new MyMXBean();
        service.validate(probe.getJmxObjectInfo());
    }

    @Test(expected=JmxRegistrationException.class)
    public void managedInterfaceNotWellNamed() {
        class MyMXBean implements JmxManageable, NotMXBeanInterface {
            public int getInt() { return 1; }

            @Override
            public JmxObjectInfo getJmxObjectInfo() {
                return new JmxObjectInfo("Test", this, MyMXBean.class);
            }
        }

        final MyMXBean probe = new MyMXBean();
        MockedJmxRegistry service = new MockedJmxRegistry();
        service.validate(probe.getJmxObjectInfo());
    }

    @Test
    public void managedInterfaceGood() {
        class MyMXBean implements JmxManageable, TestMXBean {
            public int getInt() { return 1; }

            @Override
            public JmxObjectInfo getJmxObjectInfo() {
                return new JmxObjectInfo("Test", this, MyMXBean.class);
            }
        }

        final MyMXBean probe = new MyMXBean();
        MockedJmxRegistry service = new MockedJmxRegistry();
        service.validate(probe.getJmxObjectInfo());
    }

    @Test
    public void managedSubInterfaceGood() {
        class MyMXBean implements JmxManageable, SubInterface {
            public int getInt() { return 1; }

            @Override
            public JmxObjectInfo getJmxObjectInfo() {
                return new JmxObjectInfo("Test", this, MyMXBean.class);
            }
        }

        final MyMXBean probe = new MyMXBean();
        MockedJmxRegistry service = new MockedJmxRegistry();
        service.validate(probe.getJmxObjectInfo());
    }

    @Test
    public void registerThenStart() {
        MockedJmxRegistry service = new MockedJmxRegistry();
        assertRegisteredServices(service);

        service.register(new GoodService("Alpha"));
        assertRegisteredServices(service);

        service.start();
        assertRegisteredServices(service, "com.akiban:type=Alpha");

        service.stop();
        assertRegisteredServices(service);
    }

    @Test
    public void registerAfterStarting() {
        MockedJmxRegistry service = new MockedJmxRegistry();
        assertRegisteredServices(service);

        service.start();
        service.register(new GoodService("Alpha"));
        assertRegisteredServices(service, "com.akiban:type=Alpha");

        service.stop();
        assertRegisteredServices(service);
    }

    @Test
    public void registerThenRestart() {
        MockedJmxRegistry service = new MockedJmxRegistry();
        assertRegisteredServices(service);

        service.register(new GoodService("Alpha"));
        assertRegisteredServices(service);

        service.start();
        assertRegisteredServices(service, "com.akiban:type=Alpha");

        service.stop();
        assertRegisteredServices(service);

        service.start();
        assertRegisteredServices(service, "com.akiban:type=Alpha");
    }

    @Test
    public void registerInterfaceTwice() {
        final MockedJmxRegistry service;
        try {
            service = new MockedJmxRegistry();
            assertRegisteredServices(service);
            service.register(new GoodService("Alpha"));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        service.register(new GoodService("Beta"));
        service.start();
        assertRegisteredServices(service, "com.akiban:type=Alpha", "com.akiban:type=Beta");
    }

    @Test(expected=JmxRegistrationException.class)
    public void registerNameTwice() {
        final MockedJmxRegistry service;
        try {
            service = new MockedJmxRegistry();
            assertRegisteredServices(service);
            service.register(new GoodService("Alpha"));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        service.register(new GoodService("Alpha"));
    }

    private void assertRegisteredServices(MockedJmxRegistry registry, String... actuals) {
        assertRegisteredServices(registry.getMBeanServer().getRegisteredObjectNames(), actuals);
    }

    private void assertRegisteredServices(Set<ObjectName> actual, String... expecteds) {
        Set<ObjectName> expectedSet = new HashSet<ObjectName>();
        for (String expected : expecteds) {
            try {
                expectedSet.add(new ObjectName(expected));
            } catch (MalformedObjectNameException e) {
                throw new RuntimeException(e);
            }
        }
        assertEquals("registered services", expectedSet, actual);
    }
}
