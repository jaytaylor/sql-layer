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

package com.foundationdb.server.service.plugins;

import com.google.common.io.Closeables;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

public final class JarPlugin extends Plugin {
    public static final String FILENAME_SUFFIX = ".jar";

    public static final FilenameFilter FILENAME_FILTER = new FilenameFilter() {
            @Override
            public boolean accept(File f, String name) {
                return name.endsWith(JarPlugin.FILENAME_SUFFIX);
            }
        };

    @Override
    public List<URL> getClassLoaderURLs() {
        List<URL> urls = new ArrayList<>(1);
        try {
            urls.add(pluginJar.toURI().toURL());
            File libs = new File(pluginJar.getParentFile(),
                                 pluginJar.getName().substring(0, pluginJar.getName().length() - FILENAME_SUFFIX.length()));
            if (libs.isDirectory()) {
                File[] jars = libs.listFiles(FILENAME_FILTER);
                if (jars != null) {
                    for (File jar : jars) {
                        urls.add(jar.toURI().toURL());
                    }
                }
            }
        }
        catch (MalformedURLException e) {
            throw new PluginException(e);
        }
        return urls;
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
    public static final String PROPERTY_FILE_PATH = "com/foundationdb/server/plugin-configuration.properties";
    public static final String SERVICE_CONFIG_PATH = "com/foundationdb/server/plugin-services.yaml";
}
