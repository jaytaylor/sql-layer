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

package com.foundationdb.server.types.service;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
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
            Set<Class<?>> results = new HashSet<>();
            List<String> includes = Strings.dumpResource(ConfigurableClassFinder.class, configFile);
            for (String include : includes) {
                scanDirectory(results, include);
            }
            return results;
        } catch (Exception e) {
            throw new AkibanInternalException("while looking for classes that contain functions", e);
        }
    }

    private void scanDirectory(Set<Class<?>> results, String baseUrl) throws IOException, ClassNotFoundException, URISyntaxException {
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
            public List<String> getFiles(String base) throws IOException, URISyntaxException {
                Enumeration<URL> dirContents = dirListing(base);
                List<String> results = new ArrayList<>(256); // should be plenty, but it's not too big
                while (dirContents.hasMoreElements()) {
                    URL childUrl = dirContents.nextElement();
                    buildResults(new File(childUrl.toURI()), results, base);
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

        public abstract List<String> getFiles(String base) throws IOException, URISyntaxException;
    }
}
