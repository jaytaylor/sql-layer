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

package com.akiban.server.service.functions;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p></p>Does a very simple glob-style search of classes.</p>
 * <p>Rules:
 * <ul>
 *     <li>each "glob" consists of one or more segments, delimited by dots (.)</li>
 *     <ul>
 *      <li>the last segment is a class name glob</li>
 *      <li>previous segments are package globs</li>
 *     </ul>
 *     <li>within the class name glob segment <em>only</em>, the asterisk (*) is a wildcard</li>
 * </ul>
 * </p>
 */
final class GlobularFunctionsClassFinder implements FunctionsClassFinder {

    // FunctionsClassFinder interface

    @Override
    public Set<Class<?>> findClasses() {
        try {
            Set<Class<?>> results = new HashSet<Class<?>>();
            List<String> includes = Strings.dumpResource(GlobularFunctionsClassFinder.class, configFile);
            for (String include : includes) {
                int lastDot = include.lastIndexOf(".");
                String packageName = lastDot < 0 ? "." : include.substring(0, lastDot);
                String dir = packageName.replace(".", "/") + "/";
                Pattern pattern = compilePattern(lastDot < 0 ? include : include.substring(lastDot+1));
                List<String> dirContents = Strings.dumpURLs(
                        // we have to get the union of all classes in all resources. If we were to only get
                        // the default resource for dir, then in unit/ITs we'd get the test-classes dir, not main
                        GlobularFunctionsClassFinder.class.getClassLoader().getResources(dir)
                );
                for (String file : dirContents) {
                    Matcher matcher = pattern.matcher(file);
                    if (matcher.matches()) {
                        String className = packageName + "." + matcher.group(1);
                        Class<?> theClass = GlobularFunctionsClassFinder.class.getClassLoader().loadClass(className);
                        if (!results.add(theClass)) {
                            LOG.warn("ignoring duplicate class while looking for functions: {}", theClass);
                        }
                    }
                }
            }
            return results;
        } catch (Exception e) {
            throw new AkibanInternalException("while looking for classes that contain functions", e);
        }
    }

    // GlobularFunctionsClassFinder interface

    public GlobularFunctionsClassFinder() {
        this("functionpath.txt");
    }

    // used for testing

    GlobularFunctionsClassFinder(String configFile) {
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
    private static final Logger LOG = LoggerFactory.getLogger(GlobularFunctionsClassFinder.class);
}
