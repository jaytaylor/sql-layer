
package com.akiban.server.service.plugins;

import com.google.common.io.Closeables;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

public final class JarPlugin extends Plugin {

    @Override
    public URL getClassLoaderURL() {
        try {
            return pluginJar.toURI().toURL();
        }
        catch (MalformedURLException e) {
            throw new PluginException(e);
        }
    }

    @Override
    protected Properties readPropertiesRaw() throws IOException {
        JarFile jar = new JarFile(pluginJar);
        ZipEntry configsEntry = jar.getEntry(PROPERTY_FILE_PATH);
        if (configsEntry == null)
            throw new IOException("couldn't find " + PROPERTY_FILE_PATH + " in " + jar);
        InputStream propertiesIS = jar.getInputStream(configsEntry);
        Properties result = new Properties();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(propertiesIS));
            result.load(reader);
        }
        finally {
            Closeables.closeQuietly(propertiesIS);
        }
        return result;
    }

    @Override
    public Reader getServiceConfigsReader() throws IOException {
        JarFile jar = new JarFile(pluginJar);
        ZipEntry servicesConfig = jar.getEntry(SERVICE_CONFIG_PATH);
        return new BufferedReader(new InputStreamReader(jar.getInputStream(servicesConfig)));
    }

    @Override
    public String toString() {
        return pluginJar.getAbsolutePath();
    }

    JarPlugin(File pluginJar) {
        this.pluginJar = pluginJar;
    }

    private final File pluginJar;
    private static final String PROPERTY_FILE_PATH = "com/akiban/server/plugin-configuration.properties";
    private static final String SERVICE_CONFIG_PATH = "com/akiban/server/plugin-services.yaml";
}
