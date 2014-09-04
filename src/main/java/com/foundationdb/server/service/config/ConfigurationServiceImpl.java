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

package com.foundationdb.server.service.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import com.foundationdb.server.error.BadConfigDirectoryException;
import com.foundationdb.server.error.ConfigurationPropertiesLoadException;
import com.foundationdb.server.error.ServiceNotStartedException;
import com.foundationdb.server.error.ServiceStartupException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.jmx.JmxManageable;
import com.foundationdb.server.service.plugins.Plugin;
import com.foundationdb.server.service.plugins.PluginsFinder;
import com.foundationdb.util.tap.Tap;
import com.google.inject.Inject;

public class ConfigurationServiceImpl implements ConfigurationService,
//        ConfigurationServiceMXBean, JmxManageable,
        Service {
    private final static String CONFIG_DEFAULTS_RESOURCE = "configuration-defaults.properties";
    private static final String INITIALLY_ENABLED_TAPS = "taps.initiallyenabled";

    /** Server properties. Format specified by server. */
    public static final String CONFIG_DIR_PROP = "fdbsql.config_dir"; // Note: Also in GuicedServiceManager
    public static final String SERVER_PROPERTIES_FILE_NAME = "server.properties";

    private volatile Map<String,String> properties = null;
    private final PluginsFinder pluginsFinder;
    private final Set<String> requiredKeys = new HashSet<>();

    private volatile long queryTimeoutMilli = -1L; // No timeout

    @Inject
    public ConfigurationServiceImpl(PluginsFinder pluginsFinder) {
        this.pluginsFinder = pluginsFinder;
    }

    @Override
    public long queryTimeoutMilli()
    {
        return queryTimeoutMilli;
    }

    @Override
    public void queryTimeoutMilli(long queryTimeoutMilli)
    {
        this.queryTimeoutMilli = queryTimeoutMilli;
    }

    @Override
    public boolean testing()
    {
        return false;
    }

    @Override
    public final String getProperty(String propertyName)
            throws PropertyNotDefinedException {
        String property = internalGetProperty(propertyName);
        if (property == null) {
            throw new PropertyNotDefinedException(propertyName);
        }
        return property;
    }

    private String internalGetProperty(String propertyName) {
        final Map<String, String> map = internalGetProperties();
        return map.get(propertyName);
    }

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> internalProperties = internalGetProperties();
        Map<String, String> results = new TreeMap<>();
        for (Map.Entry<String, String> entry : internalProperties.entrySet()) {
            results.put(entry.getKey(), entry.getValue());
        }
        return Collections.unmodifiableMap(results);
    }
/*
    @Override
    public long getQueryTimeoutMilli()
    {
        return queryTimeoutMilli();
    }

    @Override
    public void setQueryTimeoutMilli(long queryTimeoutMilli)
    {
        queryTimeoutMilli(queryTimeoutMilli);
    }
*/
    @Override
    public Properties deriveProperties(String withPrefix) {
        Properties properties = new Properties();
        for (Map.Entry<String,String> configProp : internalGetProperties().entrySet()) {
            String key = configProp.getKey();
            if (key.startsWith(withPrefix)) {
                properties.setProperty(
                        key.substring(withPrefix.length()),
                        configProp.getValue()
                );
            }
        }
        return properties;
    }

    @Override
    public final void start() throws ServiceStartupException {
        if (properties == null) {
            properties = internalLoadProperties();
            String initiallyEnabledTaps = properties.get(INITIALLY_ENABLED_TAPS);
            if (initiallyEnabledTaps != null) {
                Tap.setInitiallyEnabled(initiallyEnabledTaps);
            }
        }
    }

    @Override
    public final void stop() {
        try {
            unloadProperties();
        } finally {
            properties = null;
        }
    }
    
    
    @Override
    public void crash() {
        // Note: do not call unloadProperties().
        properties = null;
    }
/*
    @Override
    public final JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("Configuration", this,
                ConfigurationServiceMXBean.class);
    }
*/
    private Map<String, String> internalLoadProperties()
            throws ServiceStartupException {
        Map<String, String> ret = loadProperties();

        Set<String> missingKeys = new HashSet<>();
        for (String required : getRequiredKeys()) {
            if (!ret.containsKey(required)) {
                missingKeys.add(required);
            }
        }
        if (!missingKeys.isEmpty()) {
            throw new ServiceStartupException(String.format(
                    "Required %s not set: %s",
                    missingKeys.size() == 1 ? "property" : "properties",
                    missingKeys));
        }

        return ret;
    }
    
    /**
     * Load and return a set of configuration properties. Override this method
     * for customization in unit tests. For example, some unit tests create data
     * files in a temporary directory. These should also override
     * {@link #unloadProperties()} to clean them up.
     * @return the configuration properties
     */
    protected Map<String, String> loadProperties() {
        Properties props = null;

        props = loadResourceProperties(props);
        for (Plugin plugin : pluginsFinder.get()) {
            props = loadPluginProperties(props, plugin);
        }
        props = loadSystemProperties(props);
        props = loadConfigDirProperties(props);

        return propertiesToMap(props);
    }

    private Properties loadPluginProperties(Properties mergeInto, Plugin plugin) {
        Properties pluginProperties = plugin.readProperties();
        mergeInto.putAll(pluginProperties);
        return mergeInto;
    }

    /**
     * Override this method in unit tests to clean up any temporary files, etc.
     * A class that overrides {@link #loadProperties()} should probably also
     * override this method.
     */
    protected void unloadProperties() {

    }

    protected Set<String> getRequiredKeys() {
        return requiredKeys;
    }

    private static Map<String, String> propertiesToMap(Properties properties) {
        Map<String, String> ret = new HashMap<>();
        for (String keyStr : properties.stringPropertyNames()) {
            String value = properties.getProperty(keyStr);
            ret.put(keyStr, value);
        }
        return ret;
    }

    private static Properties chainProperties(Properties defaults) {
        return defaults == null ? new Properties() : new Properties(defaults);
    }

    private Properties loadResourceProperties(Properties defaults) {
        Properties resourceProps = chainProperties(defaults);
        try (InputStream resourceIs = ConfigurationServiceImpl.class.getResourceAsStream(CONFIG_DEFAULTS_RESOURCE)) {
            resourceProps.load(resourceIs);
        } catch (IOException e) {
            throw new ConfigurationPropertiesLoadException(CONFIG_DEFAULTS_RESOURCE, e.getMessage());
        }
        stripRequiredProperties(resourceProps, requiredKeys);
        return resourceProps;
    }

    static void stripRequiredProperties(Properties properties, Set<String> toSet) {
        Set<String> requiredKeyStrings = new HashSet<>();
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith("REQUIRED.")) {
                requiredKeyStrings.add(key);
                final String module = key.substring("REQUIRED.".length());
                for (String name : properties.getProperty(key).split(",\\s*")) {
                    toSet.add(module + '.' +name);
                }
            }
        }
        for (String key : requiredKeyStrings) {
            properties.remove(key);
        }
    }

    private static Properties loadSystemProperties(Properties defaults) {
        Properties loadedSystemProps = chainProperties(defaults);
        final Properties actualSystemProps = System.getProperties();
        for (String key : actualSystemProps.stringPropertyNames()) {
            loadedSystemProps.setProperty(key, actualSystemProps.getProperty(key));
        }
        return loadedSystemProps;
    }

    private static Properties loadConfigDirProperties(Properties defaults) {
        Properties combined = chainProperties(defaults);
        String configDirPath = combined.getProperty(CONFIG_DIR_PROP);
        if (configDirPath != null && !"NONE".equals(configDirPath)) {
            File configDir = new File(configDirPath);
            if (!configDir.exists() || !configDir.isDirectory()) {
                throw new BadConfigDirectoryException(configDir.getAbsolutePath());
            }
            try {
                loadFromFile(combined, configDirPath, SERVER_PROPERTIES_FILE_NAME);
            } catch(IOException e) {
                throw new ConfigurationPropertiesLoadException(SERVER_PROPERTIES_FILE_NAME, e.getMessage());
            }
        }
        return combined;
    }

    private static void loadFromFile(Properties props, String directory, String fileName) throws IOException {
        FileInputStream fis = null;
        try {
            File file = new File(directory, fileName);
            fis = new FileInputStream(file);
            props.load(fis);
        } finally {
            if(fis != null) {
                fis.close();
            }
        }
    }

    private Map<String, String> internalGetProperties() {
        final Map<String, String> ret = properties;
        if (ret == null) {
            throw new ServiceNotStartedException("Configuration");
        }
        return ret;
    }
}
