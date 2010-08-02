package com.akiban.cserver;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

    private final static String[] DEFAULT_SEARCH_PATH = {
            "/usr/local/vanilla/chunk-server/conf/chunkserver.properties",
            "/usr/local/etc/akiban/chunkserver.properties",
            "/etc/akiban/chunkserver.properties" };

    private final static String SEARCH_PATH_PROPERTY_NAME = "com.akiban.config";
    
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
        usingAdmin = akibanAdmin != null;
        if (usingAdmin) {
            loadFromAdmin();
        } else {
            loadFromFiles();
        }
        LOG.warn(String.format("chunkserver properties: %s", properties));
    }

    /**
     * @deprecated This is here only until Admin is fully baked into the product
     */
    boolean usingAdmin()
    {
        return usingAdmin;
    }

    private void loadFromAdmin() throws IOException
    {
        Admin admin = Admin.only();
        properties.putAll(admin.get(AdminKey.CONFIG_CHUNKSERVER).properties());
        LOG.warn(String.format("Loaded CServerConfig from %s: %s", admin.initializer(), AdminKey.CONFIG_CHUNKSERVER));
    }

    private void loadFromFiles() throws Exception
    {
        final List<String> searchPath = new ArrayList<String>();
        final String search = System.getProperty(SEARCH_PATH_PROPERTY_NAME);
        if (search != null && search.length() > 0) {
            final String[] paths = search.split(File.pathSeparator);
            searchPath.addAll(Arrays.asList(paths));
        }
        searchPath.addAll(Arrays.asList(DEFAULT_SEARCH_PATH));
        if (LOG.isInfoEnabled()) {
            LOG.info("CServerConfig search path: " + searchPath);
        }
        // Load in reverse order so that first-specified path overrides later
        // ones
        for (int index = searchPath.size(); --index >= 0;) {
            final File file = new File(searchPath.get(index));
            if (file.isFile()) {
                loadFromFile(file);
            } else if (file.isDirectory()) {
                final FilenameFilter filter = new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.endsWith(".properties");
                    }
                };
                final File[] files = file.listFiles(filter);
                for (final File child : files) {
                    if (file.isFile()) {
                        loadFromFile(child);
                    }
                }
            }
        }
    }

    private void loadFromFile(final File file) throws Exception {
        final Properties properties = new Properties();
        try {
            properties.load(new FileReader(file));
            LOG.warn(String.format("Loaded CServerConfig from %s", file));
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error(String.format("Failed to load properties from %s", file), e);
                if (exception == null) {
                    exception = e;
                }
            }
            return;
        }
        this.properties.putAll(properties);
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
     * Get the value of a property with optional default value. The default
     * value is specified with a delimiter, as in "cserver.port|8080". There is
     * subtle difference between "cserver.port" and "cserver.port|": the first
     * form returns null if there is no property named "cserver.port". The
     * second form returns an empty string.
     * 
     * @param key
     *            Key name and optional default value
     * @return The value
     */
    public String property(final String key) {
        String[] pieces = key.split("\\|");
        return properties.getProperty(pieces[0], pieces.length > 1 ? pieces[1]
                : null);
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
