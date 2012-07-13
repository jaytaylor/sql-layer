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

package com.akiban.server.types3.service;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

public final class ConfigurableClassFinder implements ClassFinder {

    // FunctionsClassFinder interface

    @Override
    public Set<Class<?>> findClasses() {
        try {
            Set<Class<?>> results = new HashSet<Class<?>>();
            List<String> includes = Strings.dumpResource(ConfigurableClassFinder.class, configFile);
            for (String include : includes) {
                scanDirectory(results, include);
            }
            return results;
        } catch (Exception e) {
            throw new AkibanInternalException("while looking for classes that contain functions", e);
        }
    }

    private void scanDirectory(Set<Class<?>> results, String baseUrl) throws IOException, ClassNotFoundException {
        URL asUrl = ConfigurableClassFinder.class.getClassLoader().getResource(baseUrl);
        if (asUrl == null)
            throw new AkibanInternalException("base url not found: " + baseUrl);
        Navigator navigator;
        try {
            navigator = Navigator.valueOf(asUrl.getProtocol().toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new AkibanInternalException("unrecognized resource type " + asUrl.getProtocol() + " for " + asUrl);
        }
        for (String filePath : navigator.getFiles(baseUrl)) {
            assert filePath.endsWith(".class") : filePath;
            int trimLength = filePath.length() - ".class".length();
            String className = filePath.replace('/', '.').substring(0, trimLength);
            Class<?> theClass = ConfigurableClassFinder.class.getClassLoader().loadClass(className);
            if (!results.add(theClass)) {
                LOG.warn("ignoring duplicate class while looking for functions: {}", theClass);
            }
        }
    }

    ConfigurableClassFinder(String configFile) {
        this.configFile = configFile;
    }

    // object state

    private final String configFile;

    // class tate
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurableClassFinder.class);

    private enum Navigator {
        JAR {
            @Override
            public List<String> getFiles(String path) throws IOException{
                List<String> dirContents = Strings.dumpURLs(dirListing(path));
                StringBuilder sb = new StringBuilder(path);
                final int baseLength = sb.length();
                for (ListIterator<String> iter = dirContents.listIterator(); iter.hasNext(); ) {
                    String content = iter.next();
                    if (!content.endsWith(".class"))
                        iter.remove();
                    else {
                        sb.setLength(baseLength);
                        sb.append(content);
                        content = sb.toString();
                        iter.set(content);
                    }
                }
                return dirContents;
            }
        },

        FILE {
            @Override
            public List<String> getFiles(String base) throws IOException {
                Enumeration<URL> dirContents = dirListing(base);
                List<String> results = new ArrayList<String>(256); // should be plenty, but it's not too big
                while (dirContents.hasMoreElements()) {
                    URL childUrl = dirContents.nextElement();
                    buildResults(new File(childUrl.getPath()), results, base);
                }
                return results;
            }

            private void buildResults(File file, List<String> results, String baseUrl) {
                assert file.exists() : "file doesn't exist: " + file;
                if (file.isFile() && file.getName().endsWith(".class")) {
                    String filePath = file.getAbsolutePath();
                    if (File.separatorChar != '/')
                        filePath = filePath.replace(File.separatorChar, '/');
                    int basePrefix = filePath.indexOf(baseUrl);
                    if (basePrefix < 0)
                        throw new AkibanInternalException(filePath + " doesn't contain " + baseUrl);
                    filePath = filePath.substring(basePrefix);
                    results.add(filePath);
                }
                else if (file.isDirectory()) {
                    File[] files = file.listFiles();
                    if (files != null) {
                        for (File child : files)
                            buildResults(child, results, baseUrl);
                    }
                }
            }
        }
        ;

        private static Enumeration<URL> dirListing(String path) throws IOException {
            // we have to get the union of all classes in all resources. If we were to only get
            // the default resource for dir, then in unit/ITs we'd get the test-classes dir, not main
            return ConfigurableClassFinder.class.getClassLoader().getResources(path);
        }

        public abstract List<String> getFiles(String base) throws IOException;
    }
}
