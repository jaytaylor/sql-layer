package com.akiban.cserver;

import java.io.File;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.admin.Admin;
import com.akiban.admin.AdminKey;

/**
 * Reads the configuration file(s) to gather up
 * 
 * @author peter
 * 
 */
public class CServerConfig {

    private static final Log LOG = LogFactory.getLog(CServerConfig.class
            .getName());

    private final static String DEFAULT_UNIT_TEST_DATA_PATH = "/tmp/data";

    private final Properties properties = new Properties();

    private boolean usingAdmin;

    private Exception exception;

    public static CServerConfig unitTestConfig() {
        CServerUtil.cleanUpDirectory(new File(DEFAULT_UNIT_TEST_DATA_PATH));
        final CServerConfig csc = new CServerConfig();
        csc.setProperty("cserver.datapath", DEFAULT_UNIT_TEST_DATA_PATH);
        csc.setProperty("cserver.fixed", "true");
        return csc;
    }

    /**
     * Loads properties from all properties files on the search path. The search
     * path is specified default locations, plus an optionally specified path
     * found in the com.akiban.config system property.
     * 
     * @throws Exception
     */
    public void load() throws Exception {
        // If akiban.admin is specified, then assume we're using admin to configure the
        // chunkserver.
        String akibanAdmin = System.getProperty(Admin.AKIBAN_ADMIN);
        if (akibanAdmin == null) {
            throw new Exception(String.format("Must specify system property %s", Admin.AKIBAN_ADMIN));
        }
        Admin admin = Admin.only();
        properties.putAll(admin.get(AdminKey.CONFIG_CHUNKSERVER).properties());
        LOG.warn(String.format("Loaded CServerConfig from %s: %s", admin.initializer(), AdminKey.CONFIG_CHUNKSERVER));
        LOG.warn(String.format("chunkserver properties: %s", properties));
    }

    /**
     * The configuration properties assembled by loading all all configuration
     * files on the search path. Empty until the {@link #load()} method has been
     * called.
     * 
     * @return the Properties
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * @return The first Exception that happened while loading from
     *         configuration files, else <tt>null</tt>.
     */
    public Exception getException() {
        return exception;
    }

    /**
     * Get the value of property with optional default value.
     * 
     * @param key
     * @param dflt
     * @return
     */
    public String property(final String key, final String dflt) {
        return properties.getProperty(key, dflt);
    }

    public void setProperty(final String key, final String value) {
        properties.setProperty(key, value);
    }
}
