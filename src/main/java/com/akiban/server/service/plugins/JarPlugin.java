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
    public ClassLoader getClassLoader() {
        URL url;
        try {
            url = pluginJar.toURI().toURL();
        }
        catch (MalformedURLException e) {
            throw new PluginException(e);
        }
        return new URLClassLoader(new URL[] { url });
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
