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

package com.foundationdb.server.service.plugins;

import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public final class PluginTest {

    @Test
    public void oneProperty() {
        checkEqual(
                map("plugins.name", "foo").and("hello", "world"),
                map("fdbsql.foo.hello", "world")
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
        tryGet(map("plugins.name", "broke").and("fdbsql.foo", "bar"));
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
        public List<URL> getClassLoaderURLs() {
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
        public List<URL> getClassLoaderURLs() {
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
