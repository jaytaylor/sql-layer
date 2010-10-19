package com.akiban.cserver.service.config;

import com.akiban.cserver.service.ServiceNotStartedException;
import com.akiban.cserver.service.ServiceStartupException;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static junit.framework.Assert.*;

public final class ConfigurationServiceImplTest {

    private static class MockConfigService extends ConfigurationServiceImpl {
        final private Property[] properties;

        MockConfigService(Property... properties) {
            assertNotNull(properties);
            this.properties = properties;
        }

        @Override
        protected Map<Property.Key, Property> internalLoadProperties() throws IOException, ServiceStartupException {
            Map<Property.Key,Property> ret = new HashMap<Property.Key, Property>(properties.length);
            for (Property property : properties) {
                ret.put(property.getKey(), property);
            }
            return ret;
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
        service.getProperty("mod1", "key1", "default1");
    }

    @Test(expected= ServiceNotStartedException.class)
    public void stoppedGetProperties() {
        ConfigurationServiceImpl service = createAndStart();
        service.stop();
        service.getProperties();
    }

    @Test(expected= ServiceNotStartedException.class)
    public void stoppedGetPropertyDefault() {
        ConfigurationServiceImpl service = createAndStart();
        service.stop();
        service.getProperty("mod1", "key1", "default1");
    }

    @Test(expected=ServiceNotStartedException.class)
    public void stoppedGetProperty() {
        ConfigurationServiceImpl service = createAndStart();
        service.stop();
        service.getProperty("mod1", "key1");
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
        String actual = service.getProperty("mod1", "key1");
        assertEquals("mod1, key1", "val1", actual);
        
        String actual2 = service.getProperty("mod1", "key1", "some default");
        assertEquals("mod1, key1", "val1", actual2);
    }

    @Test
    public void getPropertyDefinedAsNull() {
        ConfigurationServiceImpl service = createAndStart("mod1", "key1", null);
        String actual = service.getProperty("mod1", "key1");
        assertEquals("mod1, key1", null, actual);

        String actual2 = service.getProperty("mod1", "key1", "some default");
        assertEquals("mod1, key1", null, actual);
    }

    @Test(expected=PropertyNotDefinedException.class)
    public void getPropertyUnDefinedKey() {
        ConfigurationServiceImpl service = createAndStart("mod1", "key1", "val1");
        service.getProperty("mod1", "key2");
    }

    @Test(expected=PropertyNotDefinedException.class)
    public void getPropertyUnDefinedModule() {
        ConfigurationServiceImpl service = createAndStart("mod1", "key1", "val1");
        service.getProperty("mod2", "key1");
    }

    @Test
    public void getPropertyUnDefinedKeyWithDefault() {
        ConfigurationServiceImpl service = createAndStart("mod1", "key1", "val1");
        String actual = service.getProperty("mod1", "key2", "default1");
        assertEquals("mod1, key2", "default1", actual);
    }

    @Test
    public void getPropertyUnDefinedModuleWithDefault() {
        ConfigurationServiceImpl service = createAndStart("mod1", "key1", "val1");
        String actual = service.getProperty("mod2", "key1", "foobar2");
        assertEquals("mod2, key1", "foobar2", actual);
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
        expecteds.put("module.name", new Property.Key("module", "name"));
        expecteds.put("name", new Property.Key("", "name"));
        expecteds.put(".name", new Property.Key("", "name"));
        expecteds.put("name.", new Property.Key("name", ""));

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
