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

package com.akiban.server.service.servicemanager;

import com.akiban.server.service.servicemanager.configuration.ServiceConfigurationHandler;
import com.akiban.server.service.servicemanager.GuicedServiceManager.PropertyBindings;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public final class PropertyBindingsTest {

    @Test
    public void none() {
        StringsConfig config = new StringsConfig();
        new PropsBuilder().get().loadInto(config);
        compare(config);
    }

    @Test
    public void ignored() {
        StringsConfig config = new StringsConfig();
        new PropsBuilder().add("foo", "bar").get().loadInto(config);
        compare(config);
    }

    @Test
    public void goodBinding() {
        StringsConfig config = new StringsConfig();
        new PropsBuilder().add("bind:foo", "bar").get().loadInto(config);
        compare(config, "bind foo to bar");
    }

    @Test(expected = IllegalArgumentException.class)
    public void bindNoInterface() {
        StringsConfig config = new StringsConfig();
        new PropsBuilder().add("bind:", "bar").get().loadInto(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void bindEmptyImplementation() {
        StringsConfig config = new StringsConfig();
        new PropsBuilder().add("bind:foo", "").get().loadInto(config);
    }

    @Test
    public void goodRequire() {
        StringsConfig config = new StringsConfig();
        new PropsBuilder().add("require:foo", "").get().loadInto(config);
        compare(config, "require foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void valuedRequire() {
        StringsConfig config = new StringsConfig();
        new PropsBuilder().add("require:foo", "bar").get().loadInto(config);
        compare(config, "require foo");
    }

    private static void compare(StringsConfig actual, String... expected) {
        assertEquals("strings", Arrays.asList(expected), actual.messages());
    }

    private static class PropsBuilder {

        PropsBuilder add(String key, String value) {
            properties.setProperty(key, value);
            return this;
        }

        PropertyBindings get() {
            return new PropertyBindings(properties);
        }

        private final Properties properties = new Properties();
    }

    private static class StringsConfig implements ServiceConfigurationHandler {
        @Override
        public void bind(String interfaceName, String implementingClassName) {
            messages.add("bind " + interfaceName + " to " + implementingClassName);
        }

        @Override
        public void require(String interfaceName) {
            messages.add("require " + interfaceName);
        }

        @Override
        public void lock(String interfaceName) {
            messages.add("lock: " + interfaceName);
        }

        @Override
        public void mustBeLocked(String interfaceName) {
            messages.add("must be locked: " + interfaceName);
        }

        @Override
        public void mustBeBound(String interfaceName) {
            messages.add("must be bound: " + interfaceName);
        }

        @Override
        public void prioritize(String interfaceName) {
            messages.add("prioritize: " + interfaceName);
        }

        @Override
        public void sectionEnd() {
            messages.add("section end");
        }

        @Override
        public void unrecognizedCommand(String where, Object command, String message) {
            messages.add("unrecognized command");
        }

        public List<String> messages() {
            return messages;
        }

        private final List<String> messages = new ArrayList<String>();
    }
}
