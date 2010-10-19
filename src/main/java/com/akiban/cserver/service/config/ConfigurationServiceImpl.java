package com.akiban.cserver.service.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import com.akiban.admin.Admin;
import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.ServiceNotStartedException;
import com.akiban.cserver.service.ServiceStartupException;
import com.akiban.cserver.service.jmx.JmxManageable;

public class ConfigurationServiceImpl implements ConfigurationService, ConfigurationServiceMXBean, JmxManageable, Service
{
    private final static String CONFIG_DEFAULTS_RESOURCE = "configuration-defaults.properties";
    private final static String AKIBAN_ADMIN = "akiban.admin";
    /** Chunkserver properties. Format specified by chunkserver. */
    public static final String CONFIG_CHUNKSERVER = "/config/chunkserver.properties";
    private Map<Property.Key,Property> properties = null;

    private final Object INTERNAL_LOCK = new Object();

    @Override
    public final String getProperty(String module, String propertyName, String defaultValue) {
        Property property = internalGetProperty(module, propertyName);
        return (property == null) ? defaultValue : property.getValue();
    }

    @Override
    public final String getProperty(String module, String propertyName) throws PropertyNotDefinedException {
        Property property = internalGetProperty(module, propertyName);
        if (property == null) {
            throw new PropertyNotDefinedException(module, propertyName);
        }
        return property.getValue();
    }

    private Property internalGetProperty(String module, String propertyName) {
        final Map<Property.Key,Property> map = internalGetProperties();
        Property.Key key = new Property.Key(module, propertyName);
        return map.get(key);
    }

    @Override
    public final Set<Property> getProperties() {
        return new TreeSet<Property>(internalGetProperties().values());
    }

    @Override
    public final void start() throws IOException, ServiceStartupException {
        synchronized(INTERNAL_LOCK) {
            if (properties == null) {
                properties = null;
                Map<Property.Key,Property> newMap = internalLoadProperties();
                for (Map.Entry<Property.Key, Property> entry : newMap.entrySet()) {
                    if(!entry.getKey().equals(entry.getValue().getKey())) {
                        throw new ServiceStartupException(String.format(
                                "Invalidly constructed key-value pair: %s -> %s",
                                entry.getKey(), entry.getValue()));
                    }
                }
                properties = Collections.unmodifiableMap(newMap);

            }
        }
    }

    @Override
    public final void stop() {
        synchronized (INTERNAL_LOCK) {
            properties = null;
        }
    }

    @Override
    public final JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("Configuration", this, ConfigurationServiceMXBean.class);
    }

    protected Map<Property.Key, Property> internalLoadProperties() throws IOException, ServiceStartupException {
        Properties props = null;
        
        props = loadResourceProperties(props);
        props = loadSystemProperties(props);
        props = loadAdminProperties(props);

        return propetiesToMap(props);
    }

    private static Map<Property.Key, Property> propetiesToMap(Properties properties) {
        Map<Property.Key,Property> ret = new HashMap<Property.Key, Property>();
        for (String keyStr : properties.stringPropertyNames()) {
            String value = properties.getProperty(keyStr);
            Property.Key propKey = Property.parseKey(keyStr);
            ret.put(propKey, new Property(propKey, value));
        }
        return ret;
    }

    private static Properties chainProperties(Properties defaults) {
        return defaults == null ? new Properties() : new Properties(defaults);
    }

    private static Properties loadResourceProperties(Properties defaults) throws IOException {
        Properties resourceProps = chainProperties(defaults);
        InputStream resourceIs = ConfigurationServiceImpl.class.getResourceAsStream(CONFIG_DEFAULTS_RESOURCE);
        try {
            resourceProps.load(resourceIs);
        } finally {
            resourceIs.close();
        }
        return resourceProps;
    }

    private static Properties loadSystemProperties(Properties defaults) {
        Properties loadedSystemProps = chainProperties(defaults);
        final Properties actualSystemProps = System.getProperties();
        for (String key : actualSystemProps.stringPropertyNames()) {
            if (keyIsInteresting(key)) {
                loadedSystemProps.setProperty(key, actualSystemProps.getProperty(key));
            }
        }
        return loadedSystemProps;
    }

    private static Properties loadAdminProperties(Properties defaults) throws IOException, ServiceStartupException {
        Properties adminProps = chainProperties(defaults);
        final String akibanAdmin = adminProps.getProperty(AKIBAN_ADMIN);
        if (akibanAdmin != null && akibanAdmin.length() > 0) {
            final Admin admin = Admin.only();
            adminProps.putAll(admin.get(CONFIG_CHUNKSERVER).properties());
        }
        return adminProps;
    }

    private static boolean keyIsInteresting(String key) {
        return key.startsWith("akiban") || key.startsWith("persistit") || key.startsWith("cserver");
    }

    private Map<Property.Key, Property> internalGetProperties() {
        final Map<Property.Key, Property> ret;
        synchronized (INTERNAL_LOCK) {
            ret = properties;
        }
        if (ret == null) {
            throw new ServiceNotStartedException();
        }
        return ret;
    }
}
