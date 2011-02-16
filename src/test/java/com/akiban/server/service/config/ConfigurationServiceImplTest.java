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

package com.akiban.server.service.config;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import com.akiban.server.service.ServiceNotStartedException;
import com.akiban.server.service.ServiceStartupException;

public final class ConfigurationServiceImplTest {

    private static class MockConfigService extends ConfigurationServiceImpl {
        final private Property[] properties;
        private final Set<Property.Key> mockRequiredKeys = new HashSet<Property.Key>();

        MockConfigService(Property... properties) {
            assertNotNull(properties);
            this.properties = properties;
        }

        @Override
        protected Map<Property.Key, Property> loadProperties() throws IOException {
            Map<Property.Key,Property> ret = new HashMap<Property.Key, Property>(properties.length);
            for (Property property : properties) {
                ret.put(property.getKey(), property);
            }
            return ret;
        }

        public void requireKey(String name) {
            mockRequiredKeys.add(Property.parseKey(name) );
        }

        @Override
        protected Set<Property.Key> getRequiredKeys() {
            return Collections.unmodifiableSet(mockRequiredKeys);
        }
    }

    @Test(expected= ServiceNotStartedException.class)
    public void unstartedGetProperties() {
        ConfigurationServiceImpl service = new MockConfigService();
        service.getProperties();
    }

    @Test(expected=ServiceNotStartedException.class)
    public void unstartedGetProperty() {
        ConfigurationServiceImpl service = new MockConfigService();
        service.getProperty("mod1", "key1");
    }

    @Test(expected=ServiceNotStartedException.class)
    public void unstartedGetPropertyDefault() {
        ConfigurationServiceImpl service = new MockConfigService();
        service.getProperty("mod1.key1", "default1");
    }

    @Test(expected= ServiceNotStartedException.class)
    public void stoppedGetProperties() throws Exception {
        ConfigurationServiceImpl service = createAndStart();
        service.stop();
        service.getProperties();
    }

    @Test(expected= ServiceNotStartedException.class)
    public void stoppedGetPropertyDefault() throws Exception {
        ConfigurationServiceImpl service = createAndStart();
        service.stop();
        service.getProperty("mod1.key1", "default1");
    }

    @Test(expected=ServiceNotStartedException.class)
    public void stoppedGetProperty() throws Exception {
        ConfigurationServiceImpl service = createAndStart();
        service.stop();
        service.getProperty("mod1", "key1");
    }

    @Test
    public void moduleConfigForDefinedField() throws ServiceStartupException, IOException {
        ConfigurationServiceImpl service = createAndStart("mod1", "key1", "val1");
        ModuleConfiguration moduleConfig = service.getModuleConfiguration("mod1");
        service.start();
        assertEquals("mod1, key1", "val1", moduleConfig.getProperty("key1", "foo"));
    }

    @Test
    public void getModuleProperties() throws ServiceStartupException, IOException {
        ConfigurationServiceImpl service = createAndStart(
                "mod1", "key1", "val1",
                "mod1", "key2", "val2",
                "mod2", "key3", "val3");
        ModuleConfiguration moduleConfig = service.getModuleConfiguration("mod1");
        service.start();

        Properties properties = moduleConfig.getProperties();

        Properties expected = new Properties();
        expected.setProperty("key1", "val1");
        expected.setProperty("key2", "val2");

        assertEquals("properties", expected, properties);
    }

    @Test(expected=ServiceNotStartedException.class)
    public void moduleConfigUnstartedService() {
        ConfigurationService service = new MockConfigService(new Property("mod1", "key1", "val1"));
        service.getModuleConfiguration("mod1").getProperty("key1");
    }

    @Test
    public void getProperties() {
        ConfigurationServiceImpl service = createAndStart(
                "mod1", "key1", "val1",
                "mod1", "key2", "val2",
                "mod2", "key1", "val3");

        List<Property> expecteds = new ArrayList<Property>();
        expecteds.add(new Property("mod1", "key1", "val1"));
        expecteds.add(new Property("mod1", "key2", "val2"));
        expecteds.add(new Property("mod2", "key1", "val3"));

        Set<Property> actuals = service.getProperties();
        assertEquals("actuals size", 3, actuals.size());

        int i=0;
        for (Property actual : actuals) {
            final Property expected =  expecteds.get(i);
            assertEquals("property " + i, expected, actual);
            ++i;
        }
    }

    @Test
    public void getPropertyDefined() {
        ConfigurationServiceImpl service = createAndStart("mod1", "key1", "val1");
        String actual = service.getProperty("mod1.key1");
        assertEquals("mod1, key1", "val1", actual);
        
        String actual2 = service.getProperty("mod1.key1", "some default");
        assertEquals("mod1, key1", "val1", actual2);
    }

    @Test
    public void getPropertyDefinedAsNull() {
        ConfigurationServiceImpl service = createAndStart("mod1", "key1", null);
        String actual = service.getProperty("mod1.key1");
        assertEquals("mod1, key1", null, actual);

        String actual2 = service.getProperty("mod1.key1", "some default");
        assertEquals("mod1, key1", null, actual2);
    }

    @Test(expected=PropertyNotDefinedException.class)
    public void getPropertyUnDefinedKey() {
        ConfigurationServiceImpl service = createAndStart("mod1", "key1", "val1");
        service.getProperty("mod1.key2");
    }

    @Test(expected=PropertyNotDefinedException.class)
    public void getPropertyUnDefinedModule() {
        ConfigurationServiceImpl service = createAndStart("mod1", "key1", "val1");
        service.getProperty("mod2.key1");
    }

    @Test
    public void getPropertyUnDefinedKeyWithDefault() {
        ConfigurationServiceImpl service = createAndStart("mod1", "key1", "val1");
        String actual = service.getProperty("mod1.key2", "default1");
        assertEquals("mod1, key2", "default1", actual);
    }

    @Test
    public void getPropertyUnDefinedModuleWithDefault() {
        ConfigurationServiceImpl service = createAndStart("mod1", "key1", "val1");
        String actual = service.getProperty("mod2.key1", "foobar2");
        assertEquals("mod2, key1", "foobar2", actual);
    }

    @Test(expected=ServiceStartupException.class)
    public void requiredKeyNotSet() throws ServiceStartupException, IOException {
        final MockConfigService service;
        try {
            service = new MockConfigService();
            service.requireKey("a.b");
        } catch (Exception t) {
            throw new RuntimeException(t);
        }
        try {
            service.start();
        } catch (ServiceStartupException e) {
            boolean thrown = false;
            try {
                service.getProperties();
            } catch (ServiceNotStartedException e2) {
                thrown = true;
            }
            assertTrue("expected ServiceNotStartedException", thrown);
            throw e;
        }
    }

    @Test
    public void testStripRequiredProperties() {
        final String normalKey = "one.two";
        final String requiredKey = "REQUIRED.one";

        final Properties props = new Properties();
        props.setProperty(normalKey, "alpha");
        props.setProperty(requiredKey,"two,three, four");
        final Set<Property.Key> requiredKeys = new HashSet<Property.Key>();

        ConfigurationServiceImpl.stripRequiredProperties(props, requiredKeys);

        assertEquals("props.size()", 1, props.size());
        assertEquals("props[one.two]", "alpha", props.getProperty(normalKey));
        assertEquals("props[required.one]", null, props.getProperty(requiredKey));

        final Set<Property.Key> expectedRequired = new HashSet<Property.Key>();
        expectedRequired.add( Property.parseKey("one.two"));
        expectedRequired.add( Property.parseKey("one.three"));
        expectedRequired.add( Property.parseKey("one.four"));

        assertEquals("requiredKeys", expectedRequired, requiredKeys);
    }

    @Test
    public void testComparison() {
        Property p1 = new Property("a", "b", "c");
        Property p2 = new Property("b", "a", "c");
        Property p3 = new Property("b", "b", "c");
        Property p4 = new Property("b", "b", "d");
        Property p5 = new Property("a", "b", "c");

        testComparison(p1, p2, -1);
        testComparison(p2, p1, 1);

        testComparison(p2, p3, -1);
        testComparison(p3, p4, 0);
        
        testComparison(p1, p5, 0);

        assertFalse("p1.equals(p2)", p1.equals(p2));
        assertTrue("p1.equals(p5)", p1.equals(p5));
    }

    @Test
    public void testKeyParsing() {
        Map<String,Property.Key> expecteds = new HashMap<String, Property.Key>();
        expecteds.put("module.name", Property.parseKey("module.name"));
        expecteds.put("name", Property.parseKey("name"));
        expecteds.put(".name", Property.parseKey(".name"));
        expecteds.put("name.", Property.parseKey("name."));

        for (Map.Entry<String,Property.Key> entry : expecteds.entrySet()) {
            String string = entry.getKey();
            Property.Key expected = entry.getValue();
            assertEquals(string, expected, Property.parseKey(string));
        }
    }

    private static void testComparison(Property p1, Property p2, int expected) {
        if (expected > 1 || expected < -1) {
            throw new IllegalArgumentException("expected must be -1, 0 or 1. was: " + expected);
        }

        int comparison = p1.compareTo(p2);
        if (comparison < 0) {
            comparison = -1;
        }
        else if (comparison > 0) {
            comparison = 1;
        }
        assertEquals(p1 + " compared to " + p2, expected, comparison);
    }

    private static ConfigurationServiceImpl createAndStart(String... strings) {
        ConfigurationServiceImpl ret = new MockConfigService( propertiesFromStrings(strings) );
        try {
            ret.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    private static Property[] propertiesFromStrings(String... strings) {
        if (0 != (strings.length % 3)) {
            throw new IllegalArgumentException("need a multiple of three properties; found " + strings.length);
        }

        Property[] ret = new Property[strings.length / 3];
        for (int i=0; i < strings.length; i+= 3) {
            try {
                ret[i/3] = new Property(strings[i], strings[i+1], strings[i+2]);
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("at tuple #" + i, e);
            }
        }
        return ret;
    }

}
