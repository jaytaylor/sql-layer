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
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        File baseFile = file(baseUrl);
        if (!baseFile.exists())
            throw new AkibanInternalException(baseUrl + " doesn't exist (" + baseFile + ")");
        List<String> dirContents = Strings.dumpURLs(
                // we have to get the union of all classes in all resources. If we were to only get
                // the default resource for dir, then in unit/ITs we'd get the test-classes dir, not main
                ConfigurableClassFinder.class.getClassLoader().getResources(baseUrl)
        );
        for (String fileName : dirContents) {
            File file = new File(baseFile, fileName);
            String filePath = baseUrl + '/' + fileName;
            if (file.isDirectory()) {
                scanDirectory(results, filePath);
            }
            else if (file.getName().endsWith(".class")) {
                int trimLength = filePath.length() - ".class".length();
                String className = filePath.replace('/', '.').substring(0, trimLength);
                Class<?> theClass = ConfigurableClassFinder.class.getClassLoader().loadClass(className);
                if (!results.add(theClass)) {
                    LOG.warn("ignoring duplicate class while looking for functions: {}", theClass);
                }
            }
        }
    }

    private File file(String url) {
        URL asUrl = ConfigurableClassFinder.class.getClassLoader().getResource(url);
        if (asUrl == null)
            throw new AkibanInternalException("no such URL: " + url);
        try {
            return new File(asUrl.toURI());
        }
        catch (URISyntaxException e) {
            throw new AkibanInternalException("while getting URI from " + url, e);
        }
    }

    // GlobularFunctionsClassFinder interface

    public ConfigurableClassFinder() {
        this("t3s.txt");
    }

    ConfigurableClassFinder(String configFile) {
        this.configFile = configFile;
    }

    private static Pattern compilePattern(String from) {
        from = from.replace("*", ".*");
        from = "(" + from + ")" + Pattern.quote(".class");
        return Pattern.compile(from);
    }

    // object state

    private final String configFile;

    // class tate
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurableClassFinder.class);
}
