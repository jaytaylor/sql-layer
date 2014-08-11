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

package com.foundationdb.junit;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.runner.Runner;
import org.junit.runners.Parameterized;

public final class SelectedParameterizedRunner extends Parameterized {

    /**
     * <p>Parameterization override filter (works by name).</p>
     *
     * <p>If this property is set, then only parameterization names that match its value will be processed. These
     * names behave like @Failing names (in terms of regexes, etc). If this property is set and a test that matches
     * it is marked as @Failing, that test will still get run. For instance, if a parameterization named
     * <tt>myFooTest</tt> is marked as failing for a given test (either because the entire parameterization is marked
     * as failing, or because of a <tt>@Failing</tt> annotation on the method), and if you have a system property
     * <tt>{@value} == "/myFoo/"</tt>, then the test <em>will</em> be run.
     */
    public final static String PARAMETERIZATION_OVERRIDE = "fdbsql.test.param.override";
    
    private final String override;
    private final boolean overrideIsRegex;

    private final static Logger logger = LoggerFactory.getLogger(SelectedParameterizedRunner.class);
    
    public SelectedParameterizedRunner(Class<?> clazz) throws Throwable {
        super(clazz);

        override = System.getProperty(PARAMETERIZATION_OVERRIDE);
        overrideIsRegex = (override != null) && paramNameUsesRegex(override);
        if (override != null) {
            String msg = "Override is set to";
            if (overrideIsRegex) {
                msg += " regex";
            }
            msg += ":" + override;
            logger.debug(msg);
        }
    }

    static boolean paramNameUsesRegex(String paramName) {
        return paramName.length() > 2
                && (paramName.charAt(0)=='/')
                && (paramName.charAt(paramName.length()-1)=='/');
    }
    
    /**
     * Returns whether a given parameterization matches a given regex. The regex should be in "/regex/" format.
     * @param paramName the haystack, as it were
     * @param paramRegex a string that starts and ends with '/', and between them has a needle.
     * @return whether the paramRegex is found in paramName
     */
    static boolean paramNameMatchesRegex(String paramName, String paramRegex)
    {
        assert paramRegex.charAt(0)=='/';
        assert paramRegex.charAt(paramRegex.length()-1)=='/';
        assert paramRegex.length() > 2;
        String regex = paramRegex.substring(1, paramRegex.length()-1);
        return Pattern.compile(regex).matcher(paramName).find();
    }

    @Override
    protected List<Runner> getChildren() {
        List<Runner> children = super.getChildren();        
        
        if (override != null) {
            for (Iterator<Runner> iterator = children.iterator(); iterator.hasNext(); ) {
                Object child = iterator.next();
                try {
                    Field f;
                    try {
                        f = child.getClass().getDeclaredField("fName");
                    }
                    catch (NoSuchFieldException e) {
                        continue;
                    }
                    f.setAccessible(true);
                    String fName = (String)f.get(child);
                    if (overrideIsRegex && !paramNameMatchesRegex(fName, override)) {
                        iterator.remove();
                    }
                    else if (!overrideIsRegex && !fName.equals(override))
                        iterator.remove();
                    }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }        
        return children;
    }
}
