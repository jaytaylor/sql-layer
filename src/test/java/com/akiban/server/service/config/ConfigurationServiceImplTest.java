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

import com.akiban.server.error.ServiceNotStartedException;
import com.akiban.server.error.ServiceStartupException;

public final class ConfigurationServiceImplTest {

    private static class MockConfigService extends ConfigurationServiceImpl {
        final private Property[] properties;
        private final Set<String> mockRequiredKeys = new HashSet<String>();

        MockConfigService(Property... properties) {
            assertNotNull(properties);
            this.properties = properties;
        }

        @Override
        protected Map<String, Property> loadProperties() {
            Map<String,Property> ret = new HashMap<String, Property>(properties.length);
            for (Property property : properties) {
                ret.put(property.getKey(), property);
            }
            return ret;
        }

        public void requireKey(String name) {
            mockRequiredKeys.add(name);
        }

        @Override
        protected Set<String> getRequiredKeys() {
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
        service.getProperty("mod1");
    }

    @Test(expected=ServiceNotStartedException.class)
    public void unstartedGetPropertyDefault() {
        ConfigurationServiceImpl service = new MockConfigService();
        service.getProperty("mod1.key1");
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
        service.getProperty("mod1.key1");
    }

    @Test(expected=ServiceNotStartedException.class)
    public void stoppedGetProperty() throws Exception {
        ConfigurationServiceImpl service = createAndStart();
        service.stop();
        service.getProperty("mod1");
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
        
        String actual2 = service.getProperty("mod1.key1");
        assertEquals("mod1, key1", "val1", actual2);
    }

    @Test
    public void getPropertyDefinedAsNull() {
        ConfigurationServiceImpl service = createAndStart("mod1", "key1", null);
        String actual = service.getProperty("mod1.key1");
        assertEquals("mod1, key1", null, actual);

        String actual2 = service.getProperty("mod1.key1");
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
    public void prefixProperties() {
        ConfigurationServiceImpl service = createAndStart(
                "a", "one", "1",
                "a", "one.alpha", "1a",
                "a", "two", "2",
                "b", "three", "3"
        );
        Properties props = service.deriveProperties("a.");
        assertEquals("properties.size()", 3, props.size());
        assertEquals("properties[one]", "1", props.getProperty("one"));
        assertEquals("properties[one.alpha]", "1a", props.getProperty("one.alpha"));
        assertEquals("properties[two]", "2", props.getProperty("two"));
    }

    @Test
    public void prefixPropertiesEmptyPrefix() {
        ConfigurationServiceImpl service = createAndStart(
                "a", "one", "1",
                "a", "one.alpha", "1a",
                "a", "two", "2",
                "b", "three", "3"
        );
        Properties props = service.deriveProperties("");
        assertEquals("properties.size()", 4, props.size());
        assertEquals("properties[a.one]", "1", props.getProperty("a.one"));
        assertEquals("properties[a.one.alpha]", "1a", props.getProperty("a.one.alpha"));
        assertEquals("properties[a.two]", "2", props.getProperty("a.two"));
        assertEquals("properties[a.three]", "3", props.getProperty("b.three"));
    }

    @Test
    public void prefixPropertiesUnmatchedPrefix() {
        ConfigurationServiceImpl service = createAndStart(
                "a", "one", "1",
                "a", "one.alpha", "1a",
                "a", "two", "2",
                "b", "three", "3"
        );
        Properties props = service.deriveProperties("c.");
        assertEquals("properties.size()", 0, props.size());
    }

    @Test(expected = NullPointerException.class)
    public void prefixPropertiesNullPrefix() {
        ConfigurationServiceImpl service = createAndStart(
                "a", "one", "1",
                "a", "one.alpha", "1a",
                "a", "two", "2",
                "b", "three", "3"
        );
        service.deriveProperties(null);
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
        final Set<String> requiredKeys = new HashSet<String>();

        ConfigurationServiceImpl.stripRequiredProperties(props, requiredKeys);

        assertEquals("props.size()", 1, props.size());
        assertEquals("props[one.two]", "alpha", props.getProperty(normalKey));
        assertEquals("props[required.one]", null, props.getProperty(requiredKey));

        final Set<String> expectedRequired = new HashSet<String>();
        expectedRequired.add("one.two");
        expectedRequired.add("one.three");
        expectedRequired.add("one.four");

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
