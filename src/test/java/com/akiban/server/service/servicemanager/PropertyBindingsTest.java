/**
 * Copyright (C) 2011 Akiban Technologies Inc.
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

package com.akiban.server.service.servicemanager;

import com.akiban.server.service.servicemanager.configuration.ServiceBinding;
import com.akiban.server.service.servicemanager.GuicedServiceManager.PropertyBindings;
import com.akiban.server.service.servicemanager.configuration.yaml.SectionalConfigurationStrategy;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

    private static class StringsConfig implements SectionalConfigurationStrategy {
        @Override
        public void bind(String interfaceName, String implementingClassName) {
            messages.add("bind " + interfaceName + " to " + implementingClassName);
        }

        @Override
        public void require(String interfaceName) {
            messages.add("require " + interfaceName);
        }

        @Override
        public Collection<ServiceBinding> serviceBindings() {
            throw new UnsupportedOperationException();
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
