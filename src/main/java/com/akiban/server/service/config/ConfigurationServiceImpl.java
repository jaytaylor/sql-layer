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
import java.util.TreeSet;

import com.akiban.server.error.BadConfigDirectoryException;
import com.akiban.server.error.ConfigurationPropertiesLoadException;
import com.akiban.server.error.ServiceNotStartedException;
import com.akiban.server.error.ServiceStartupException;
import com.akiban.server.service.Service;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.util.tap.Tap;

public class ConfigurationServiceImpl implements ConfigurationService,
        ConfigurationServiceMXBean, JmxManageable,
        Service {
    private final static String CONFIG_DEFAULTS_RESOURCE = "configuration-defaults.properties";
    private static final String INITIALLY_ENABLED_TAPS = "taps.initiallyenabled";

    /** Server properties. Format specified by server. */
    public static final String CONFIG_DIR_PROP = "akserver.config_dir";
    public static final String CONFIG_SERVER = "/config/server.properties";

    private Map<String,Property> properties = null;
    private final Set<String> requiredKeys = new HashSet<String>();

    private final Object INTERNAL_LOCK = new Object();
    
    private volatile long queryTimeoutSec = -1L; // No timeout

    @Override
    public long queryTimeoutSec()
    {
        return queryTimeoutSec;
    }

    @Override
    public void queryTimeoutSec(long queryTimeoutSec)
    {
        this.queryTimeoutSec = queryTimeoutSec;
    }

    @Override
    public boolean testing()
    {
        return false;
    }

    @Override
    public final String getProperty(String propertyName)
            throws PropertyNotDefinedException {
        Property property = internalGetProperty(propertyName);
        if (property == null) {
            throw new PropertyNotDefinedException(propertyName);
        }
        return property.getValue();
    }

    private Property internalGetProperty(String propertyName) {
        final Map<String, Property> map = internalGetProperties();
        return map.get(propertyName);
    }

    @Override
    public final Set<Property> getProperties() {
        return new TreeSet<Property>(internalGetProperties().values());
    }

    @Override
    public long getQueryTimeoutSec()
    {
        return queryTimeoutSec();
    }

    @Override
    public void setQueryTimeoutSec(long queryTimeoutSec)
    {
        queryTimeoutSec(queryTimeoutSec);
    }

    @Override
    public Properties deriveProperties(String withPrefix) {
        Properties properties = new Properties();
        for (Property configProp : internalGetProperties().values()) {
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
        synchronized (INTERNAL_LOCK) {
            if (properties == null) {
                properties = null;
                Map<String, Property> newMap = internalLoadProperties();
                for (Map.Entry<String, Property> entry : newMap.entrySet()) {
                    if (!entry.getKey().equals(entry.getValue().getKey())) {
                        throw new ServiceStartupException(
                                String.format(
                                        "Invalidly constructed key-value pair: %s -> %s",
                                        entry.getKey(), entry.getValue()));
                    }
                }
                properties = Collections.unmodifiableMap(newMap);
            }
            Property initiallyEnabledTaps = properties.get(INITIALLY_ENABLED_TAPS);
            if (initiallyEnabledTaps != null) {
                Tap.setInitiallyEnabled(initiallyEnabledTaps.getValue());
            }
        }
    }

    @Override
    public final void stop() {
        try {
            unloadProperties();
        } finally {
            synchronized (INTERNAL_LOCK) {
                properties = null;
            }
        }
    }
    
    
    @Override
    public void crash() {
        // Note: do not call unloadProperties().
        synchronized (INTERNAL_LOCK) {
            properties = null;
        }
    }

    @Override
    public final JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("Configuration", this,
                ConfigurationServiceMXBean.class);
    }

    private Map<String, Property> internalLoadProperties()
            throws ServiceStartupException {
        Map<String, Property> ret = loadProperties();

        Set<String> missingKeys = new HashSet<String>();
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
    protected Map<String, Property> loadProperties() {
        Properties props = null;

        props = loadResourceProperties(props);
        props = loadSystemProperties(props);
        if (shouldLoadConfigDirProperties()) {
            props = loadConfigDirProperties(props);
        }

        return propertiesToMap(props);
    }

    /**
     * Override this method in unit tests to clean up any temporary files, etc.
     * A class that overrides {@link #loadProperties()} should probably also
     * override this method.
     */
    protected void unloadProperties() {

    }

    protected boolean shouldLoadConfigDirProperties() {
        return true;
    }

    protected Set<String> getRequiredKeys() {
        return requiredKeys;
    }

    private static Map<String, Property> propertiesToMap(
            Properties properties) {
        Map<String, Property> ret = new HashMap<String, Property>();
        for (String keyStr : properties.stringPropertyNames()) {
            String value = properties.getProperty(keyStr);
            ret.put(keyStr, new Property(keyStr, value));
        }
        return ret;
    }

    private static Properties chainProperties(Properties defaults) {
        return defaults == null ? new Properties() : new Properties(defaults);
    }

    private Properties loadResourceProperties(Properties defaults) {
        Properties resourceProps = chainProperties(defaults);
        InputStream resourceIs = ConfigurationServiceImpl.class
                .getResourceAsStream(CONFIG_DEFAULTS_RESOURCE);
        try {
            resourceProps.load(resourceIs);
        } catch (IOException e) {
            throw new ConfigurationPropertiesLoadException(CONFIG_DEFAULTS_RESOURCE, e.getMessage());
        } finally {
            try {
                resourceIs.close();
            } catch (IOException e) {
                //TODO: InputStream#close() doesn't do anything. 
                // how to handle the "impossible" situation? 
            }
        }
        stripRequiredProperties(resourceProps, requiredKeys);
        return resourceProps;
    }

    static void stripRequiredProperties(Properties properties, Set<String> toSet) {
        Set<String> requiredKeyStrings = new HashSet<String>();
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
            if (keyIsInteresting(key)) {
                loadedSystemProps.setProperty(key,
                        actualSystemProps.getProperty(key));
            }
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
                loadFromFile(combined, configDirPath, CONFIG_SERVER);
            } catch(IOException e) {
                throw new ConfigurationPropertiesLoadException(CONFIG_SERVER, e.getMessage());
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

    private static boolean keyIsInteresting(String key) {
        return key.startsWith("akiban") || key.startsWith("persistit")
                || key.startsWith("akserver");
    }

    private Map<String, Property> internalGetProperties() {
        final Map<String, Property> ret;
        synchronized (INTERNAL_LOCK) {
            ret = properties;
        }
        if (ret == null) {
            throw new ServiceNotStartedException("Configuration");
        }
        return ret;
    }
}
