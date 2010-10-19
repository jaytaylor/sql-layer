package com.akiban.cserver.service.config;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.ServiceNotStartedException;
import com.akiban.cserver.service.jmx.JmxManageable;

public class ConfigurationServiceImpl implements ConfigurationService, ConfigurationServiceMXBean, Service
{
    private final static String DEFAULT_CONFIG_FILE = "configuration.properties";
    private final Set<Property> valuesSet;
    private final ConcurrentHashMap<String,Map<String,Property>> values;
    private final Object INTERNAL_LOCK = new Object();

    private Set<Property> valuesAsSet = null;
    private ConcurrentHashMap<String,Map<String,Property>> maps = null;
    static final String UNCATEGORIZED_PROPERTY = "UNCATEGORIZED";

    public static ConfigurationServiceImpl emptyConfigurationService() {
        return new ConfigurationServiceImpl(new Property[0]);
    }

    public ConfigurationServiceImpl() {
        this(convertProperties(getDefaultProperties()));
    }

    private static Properties getDefaultProperties() {
        InputStream is = ConfigurationServiceImpl.class.getResourceAsStream(DEFAULT_CONFIG_FILE);
        Properties props = new Properties();
        try {
            if (is == null) {
                throw new FileNotFoundException(DEFAULT_CONFIG_FILE + " not found");
            }
            try {
                props.load(is);
            }
            finally {
                is.close();
            }
        } catch (Exception e) {
            props.setProperty("ERROR.message", e.getMessage());
            props.setProperty("ERROR.class", e.getClass().getName());
            StackTraceElement[] trace = e.getStackTrace();
            for (int i=0; i<trace.length; ++i) {
                props.setProperty(String.format("ERROR.stack%02d", i), trace[i].toString());
            }
        }
        return props;
    }

    ConfigurationServiceImpl(Property... properties) {
        Set<Property> tmpSet = new TreeSet<Property>();

        for (Property property : properties) {
            tmpSet.add(property);
        }

        ConcurrentHashMap<String, Map<String, Property>> tmpMap = new ConcurrentHashMap<String, Map<String, Property>>();
        for (Property property : tmpSet) {
            Map<String,Property> map = tmpMap.get( property.getModule() );
            if (map == null) {
                map = new HashMap<String, Property>();
                map.put(property.getName(), property);
                tmpMap.put(property.getModule(), Collections.synchronizedMap(map));
            }
            else {
                map.put(property.getName(), property);
            }
        }

        values = tmpMap;
        valuesSet = tmpSet;
    }

    static Property[] convertProperties(Properties properties) {
        List<Property> retList = new ArrayList<Property>(properties.size());
        for (Map.Entry<Object,Object> entry : properties.entrySet()) {
            final Object key = entry.getKey();
            final Object value = entry.getValue();
            if ( (key instanceof String) && (value instanceof String) ) {
                String keyStr = (String)key;
                int firstDot = keyStr.indexOf('.');
                final String module;
                final String propertyName;
                if (firstDot <= 0 || (firstDot == keyStr.length()-1)) {
                    module = UNCATEGORIZED_PROPERTY;
                    propertyName = keyStr;
                }
                else {
                    module = keyStr.substring(0, firstDot);
                    propertyName = keyStr.substring(firstDot+1);
                }

                retList.add(new Property(module, propertyName, (String)value));
            }
        }
        return retList.toArray(new Property[retList.size()]);
    }

    /**
     * Given a module, returns its thread-safe properties map, or null if no such map exists.
     * @param module the module name
     * @return a Map which is  thread safe.
     */
    private Map<String,Property> mapFor(String module) {
        return getMaps().get(module);
    }

    @Override
    public String getProperty(String module, String propertyName) {
        final Map<String,Property> map = mapFor(module);
        if (map == null) {
            throw new PropertyNotDefinedException(module, propertyName);
        }
        Property property = map.get(propertyName);
        if (property == null) {
            throw new PropertyNotDefinedException(module, propertyName);
        }
        return property.getValue();
    }

    @Override
    public Map<String, Property> getProperties(String module) {
        final Map<String,Property> map = mapFor(module);
        return (map == null)
                ? Collections.<String,Property>emptyMap()
                : Collections.unmodifiableMap(map);
    }

    /**
     * For convenience of viewing, the resulting Set will be sorted by namespace and property name.
     * @return the properties, sorted by namespace and property name
     */
    @Override
    public Set<Property> getProperties() {
        return Collections.unmodifiableSet(getPropertiesSet());
    }

    @Override
    public void start() {
        synchronized (INTERNAL_LOCK) {
            maps = values;
            valuesAsSet = valuesSet;
        }
    }

    @Override
    public void stop() {
        synchronized (INTERNAL_LOCK) {
            maps = null;
            valuesAsSet = null;
        }
    }

    private ConcurrentHashMap<String,Map<String,Property>> getMaps() {
        final ConcurrentHashMap<String,Map<String,Property>> localMaps;
        synchronized(INTERNAL_LOCK) {
            localMaps = maps;
        }
        if (localMaps == null) {
            throw new ServiceNotStartedException();
        }
        return localMaps;
    }

    private Set<Property> getPropertiesSet() {
        final Set<Property> localSet;
        synchronized(INTERNAL_LOCK) {
            localSet = valuesAsSet;
        }
        if (valuesAsSet == null) {
            throw new ServiceNotStartedException();
        }
        return valuesAsSet;
    }

}
