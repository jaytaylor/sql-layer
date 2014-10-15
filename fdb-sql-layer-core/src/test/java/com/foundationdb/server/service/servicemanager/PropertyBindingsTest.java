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

package com.foundationdb.server.service.servicemanager;

import com.foundationdb.server.service.servicemanager.configuration.ServiceConfigurationHandler;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager.PropertyBindings;
import com.google.inject.Module;
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

    @Test
    public void prioritize() {
        StringsConfig config = new StringsConfig();
        new PropsBuilder().add("require:foo", "").add("require:bar", "").add("prioritize:foo", "").get().loadInto(config);
        compare(config, "require foo", "prioritize foo", "require bar");
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
        public void bind(String interfaceName, String implementingClassName, ClassLoader ignored) {
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
            messages.add("prioritize " + interfaceName);
        }

        @Override
        public void sectionEnd() {
            messages.add("section end");
        }

        @Override
        public void unrecognizedCommand(String where, Object command, String message) {
            messages.add("unrecognized command");
        }

        @Override
        public void bindModules(List<Module> modules) {
            for (Module module : modules)
                messages.add("binding module " + module.getClass());
        }

        @Override
        public void bindModulesError(String where, Object command, String message) {
            messages().add("bind-modules error");
        }

        @Override
        public void unbind(String interfaceName) {
            messages.add("unbind " + interfaceName);
        }

        public List<String> messages() {
            return messages;
        }

        private final List<String> messages = new ArrayList<>();
    }
}
