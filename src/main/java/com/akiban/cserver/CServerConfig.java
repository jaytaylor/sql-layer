package com.akiban.cserver;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

	private final Properties properties = new Properties();

	private final List<String> configFileNames = new ArrayList<String>();

	private Exception exception;
	
	public static CServerConfig unitTestConfig() {
		final CServerConfig csc = new CServerConfig();
		csc.properties.setProperty("unit_test", "true");
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

	public void loadFromFile(final File file) throws Exception {
		final Properties properties = new Properties();
		try {
			properties.load(new FileReader(file));
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Failed to load properties from: " + file, e);
				if (exception == null) {
					exception = e;
				}
			}
			return;
		}
		this.properties.putAll(properties);
		configFileNames.add(file.getAbsolutePath());
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
	 * @return The list of configuration file names.
	 */
	public List<String> getConfigFileNames() {
		return configFileNames;
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
}
