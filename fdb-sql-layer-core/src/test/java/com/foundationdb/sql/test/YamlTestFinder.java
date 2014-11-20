/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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

package com.foundationdb.sql.test;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;
import java.util.TimeZone;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import org.joda.time.DateTimeZone;

public class YamlTestFinder
{
    static {
        String timezone = "UTC";
        DateTimeZone.setDefault(DateTimeZone.forID(timezone));
        TimeZone.setDefault(TimeZone.getTimeZone(timezone));
    }

    private static final String PROPERTY_PREFIX = "com.foundationdb.sql.test.yaml";

    /**
     * A regular expression matching the names of the YAML files in the
     * resource directory, not including the extension, to use for tests.
     */
    private static final String CASE_NAME_REGEXP_PROPERTY = PROPERTY_PREFIX + ".CASE_NAME_REGEXP";

    /** The directory containing the YAML files. */
    private static final String RESOURCE_DIR_PROPERTY = PROPERTY_PREFIX + ".RESOURCE_DIR";

    /** Whether to search the resource directory recursively for test files. */
    private static final String RECURSIVE_PROPERTY = PROPERTY_PREFIX + ".RECURSIVE";

    /** A resource known to be in the root to look for and find where the rest are. */
    private static final String RESOURCE_MARKER = "/com/foundationdb/sql/test/yaml/README";

    private YamlTestFinder() {
    }
    
    /** Return a collection of class instantiation parameters for YAML tests.
     * The parameters are: <code>String caseName, URL url</code>.
     */
    public static Iterable<Object[]> findTests() throws Exception {
        String caseNameRegexp = System.getProperty(CASE_NAME_REGEXP_PROPERTY, "test-.*");
        Pattern filenamePattern = Pattern.compile(caseNameRegexp + "[.]yaml");
        boolean recursive = Boolean.valueOf(System.getProperty(RECURSIVE_PROPERTY, "true"));
        String resourceDir = System.getProperty(RESOURCE_DIR_PROPERTY);
        
        Collection<Object[]> params = new ArrayList<>();

        if (resourceDir != null) {
            // User-specified file location.
            collectFiles(params, new File(resourceDir),
                         recursive, filenamePattern);
        }
        else {
            URL url = YamlTestFinder.class.getResource(RESOURCE_MARKER);
            if (url == null) {
                throw new RuntimeException("Problem finding tests: " + RESOURCE_MARKER);
            }
            if ("file".equals(url.getProtocol())) {
                // Maven-specified file location.
                collectFiles(params, new File(url.getPath()).getParentFile(),
                             true, filenamePattern);
            }
            else {
                // Inside test.jar.
                collectResources(params, url, filenamePattern);
            }
        }
        return params;
    }

    /**
     * Add files from the directory that match the pattern to params, recursing
     * if appropriate.
     */
    private static void collectFiles(final Collection<Object[]> params, File directory,
                                     final boolean recursive, final Pattern pattern) {
        File[] files = directory.listFiles(
            new FileFilter() {
                @Override
                public boolean accept(File file) {
                    if (file.isDirectory()) {
                        if (recursive) {
                            collectFiles(params, file, recursive, pattern);
                        }
                    }
                    else {
                        String name = file.getName();
                        if (pattern.matcher(name).matches()) {
                            try {
                                params.add(new Object[] {
                                               name.substring(0, name.length() - 5),
                                               file.toURI().toURL()
                                           });
                            }
                            catch (MalformedURLException ex) {
                                throw new RuntimeException(ex);
                            }
                        }
                    }
                    return false;
                }
            }
        );
        if (files == null) {
            throw new RuntimeException("Problem accessing directory: " + directory);
        }
    }

    // In normal operation, where mvn test is run from the sql-layer
    // root, Maven's reactor will substitute access to files in the
    // sibling module, which is much nicer for iterative development.
    // This is for the case where the test-jar is actually being used,
    // such as when mvn test is run in the fdb-sql-layer-core child.
    private static void collectResources(Collection<Object[]> params, URL url,
                                         Pattern pattern) {
        String fullURL = url.toString();
        int bang = fullURL.indexOf('!');
        if (!fullURL.startsWith("jar:file:") ||
            (bang < 0)) {
            throw new RuntimeException("Unexpected resource location: " + fullURL);
        }
        String jarFilename = fullURL.substring(9, bang);
        try {
            JarFile jarFile = new JarFile(jarFilename);
            Enumeration<JarEntry> e = jarFile.entries();
            while (e.hasMoreElements()) {
                JarEntry jarEntry = e.nextElement();
                if (jarEntry.isDirectory()) continue;
                String filename = jarEntry.getName();
                int idx = filename.lastIndexOf('/');
                String name = (idx < 0) ? filename : filename.substring(idx+1);
                if (pattern.matcher(name).matches()) {
                    params.add(new Object[] {
                                   name.substring(0, name.length() - 5),
                                   new URL("jar:file:" + jarFilename +
                                           "!/" + filename)
                               });
                }
            }
        }
        catch (MalformedURLException ex) {
            throw new RuntimeException(ex);
        }
        catch (IOException ex) {
            throw new RuntimeException("Error reading from " + jarFilename, ex);
        }
    }

}