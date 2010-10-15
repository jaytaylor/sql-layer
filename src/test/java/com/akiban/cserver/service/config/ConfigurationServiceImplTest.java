package com.akiban.cserver.service.config;

import com.akiban.cserver.service.ServiceNotStartedException;
import org.junit.Test;

import java.util.*;

import static junit.framework.Assert.*;
import static com.akiban.cserver.service.config.ConfigurationServiceImpl.UNCATEGORIZED_PROPERTY;

public final class ConfigurationServiceImplTest {

    @Test(expected= ServiceNotStartedException.class)
    public void unstartedGetProperties() {
        ConfigurationServiceImpl service = ConfigurationServiceImpl.emptyConfigurationService();
        service.getProperties();
    }

    @Test(expected= ServiceNotStartedException.class)
    public void unstartedGetPropertiesModule() {
        ConfigurationServiceImpl service = ConfigurationServiceImpl.emptyConfigurationService();
        service.getProperties("mod1");
    }

    @Test(expected=ServiceNotStartedException.class)
    public void unstartedGetProperty() {
        ConfigurationServiceImpl service = ConfigurationServiceImpl.emptyConfigurationService();
        service.getProperty("mod1", "key1");
    }

    @Test(expected= ServiceNotStartedException.class)
    public void stoppedGetProperties() {
        ConfigurationServiceImpl service = createAndStart();
        service.stop();
        service.getProperties();
    }

    @Test(expected= ServiceNotStartedException.class)
    public void stoppedGetPropertiesModule() {
        ConfigurationServiceImpl service = createAndStart();
        service.stop();
        service.getProperties("mod1");
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
    public void getPropertiesForModule() {
        ConfigurationServiceImpl service = createAndStart(
                "mod1", "key1", "val1",
                "mod1", "key2", "val2",
                "mod2", "key1", "val3");

        List<Property> expecteds = new ArrayList<Property>();
        expecteds.add(new Property("mod1", "key1", "val1"));
        expecteds.add(new Property("mod1", "key2", "val2"));

        Map<String,Property> actuals = service.getProperties("mod1");
        assertEquals("actuals size", 2, actuals.size());

        assertEquals("key1", expecteds.get(0), actuals.get("key1"));
        assertEquals("key1", expecteds.get(1), actuals.get("key2"));
    }

    @Test
    public void getPropertyDefined() {
        ConfigurationServiceImpl service = createAndStart("mod1", "key1", "val1");
        String actual = service.getProperty("mod1", "key1");
        assertEquals("mod1, key1", "val1", actual);
    }

    @Test(expected=PropertyNotDefinedException.class)
    public void getPropertyUnDefinedKey() {
        ConfigurationServiceImpl service = createAndStart("mod1", "key1", "val1");
        String actual = service.getProperty("mod1", "key2");
        assertEquals("mod1, key2", "foobar", actual);
    }

    @Test(expected=PropertyNotDefinedException.class)
    public void getPropertyUnDefinedModule() {
        ConfigurationServiceImpl service = createAndStart("mod1", "key1", "val1");
        String actual = service.getProperty("mod2", "key1");
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
    public void testPropertyConversions() {
        assertEquals("module.name=val1", new Property("module", "name", "val1"), convertProperty("module.name", "val1"));
        assertEquals("name=val2", new Property(UNCATEGORIZED_PROPERTY, "name", "val2"), convertProperty("name", "val2"));
        assertEquals(".name=val2", new Property(UNCATEGORIZED_PROPERTY, ".name", "val3"), convertProperty(".name", "val3"));
        assertEquals("name.=val2", new Property(UNCATEGORIZED_PROPERTY, "name.", "val3"), convertProperty("name.", "val3"));
    }

    private static Property convertProperty(String name, String value) {
        Properties props = new Properties();
        props.setProperty(name, value);
        Property[] actual = ConfigurationServiceImpl.convertProperties(props);
        assertEquals("actual.length", 1, actual.length);
        return actual[0];
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
        ConfigurationServiceImpl ret = new ConfigurationServiceImpl( propertiesFromStrings(strings) );
        ret.start();
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
