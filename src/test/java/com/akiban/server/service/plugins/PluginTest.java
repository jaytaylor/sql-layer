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

package com.akiban.server.service.plugins;

import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public final class PluginTest {

    @Test
    public void oneProperty() {
        checkEqual(
                map("plugins.name", "foo").and("hello", "world"),
                map("plugins.foo.hello", "world")
        );
    }

    @Test
    public void noProperties() {
        checkEqual(
                map("plugins.name", "foo"),
                empty()
        );
    }

    @Test(expected = PluginException.class)
    public void noName() {
        tryGet(map("bar", "foo"));
    }

    @Test(expected = PluginException.class)
    public void invalidName() {
        tryGet(map("plugins.name", "foo."));
    }

    @Test(expected = PluginException.class)
    public void usingReservedKeys() {
        tryGet(map("plugins.name", "broke").and("plugins.foo", "bar"));
    }

    @Test(expected = PluginException.class)
    public void getRawBreaks() {
        new ExceptionalPlugin().readProperties();
    }

    private void checkEqual(BuildableProperties input, BuildableProperties expected) {
        DummyPlugin plugin = new DummyPlugin(input.properties);
        Properties output = plugin.readProperties();
        assertEquals(expected.properties, output);
    }

    private void tryGet(BuildableProperties input) {
        new DummyPlugin(input.properties).readProperties();
    }

    private BuildableProperties empty() {
        return new BuildableProperties();
    }

    private static BuildableProperties map(Object key, Object value) {
        BuildableProperties properties = new BuildableProperties();
        properties.and(key, value);
        return properties;
    }

    private static class DummyPlugin extends Plugin {

        @Override
        protected Properties readPropertiesRaw() throws IOException {
            return properties;
        }

        @Override
        public URL getClassLoaderURL() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Reader getServiceConfigsReader() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return "Dummy" + properties;
        }

        private DummyPlugin(Properties properties) {
            this.properties = properties;
        }

        private final Properties properties;

    }

    private static class ExceptionalPlugin extends Plugin {

        @Override
        public URL getClassLoaderURL() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Properties readPropertiesRaw() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Reader getServiceConfigsReader() {
            throw new UnsupportedOperationException();
        }
    }

    private static class BuildableProperties {

        BuildableProperties and(Object key, Object value) {
            properties.put(key, value);
            return this;
        }

        private final Properties properties = new Properties();
    }
}
