/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * String utils.
 */
public abstract class Strings {

    /**
     * Gets the system <tt>line.separator</tt> newline.
     * @return <tt>System.getProperty("line.separator")</tt>
     */
    public static String nl() {
        String nl = System.getProperty("line.separator");
        if (nl == null) {
            throw new NullPointerException("couldn't find system property line.separator");
        }
        return nl;
    }

    /**
     * Joins the given Strings into a single, newline-delimited String. Newline is the system-dependent one as
     * defined by the system property <tt>line.separator</tt>.
     * @param strings the strings
     * @return the String
     */
    public static String join(List<?> strings) {
        return join(strings, nl());
    }

    /**
     * Joins the given Strings into a single, newline-delimited String. Newline is the system-dependent one as
     * defined by the system property <tt>line.separator</tt>.
     * @param strings the strings
     * @return the String
     */
    public static String join(String... strings) {
        return join(Arrays.asList(strings));
    }

    /**
     * Joins the given Strings into a single String with the given delimiter. The last String in the list will
     * not have the delimiter appended. If the list is empty, this returns an empty string.
     * @param strings a list of strings. May not be null.
     * @param delimiter the delimiter between strings; this will be inserted <tt>(strings.size() - 1)</tt> times.
     * May not be null.
     * @return the joined string
     */
    public static String join(List<?> strings, String delimiter) {
        if (strings.size() == 0) {
            return "";
        }
        StringBuilder builder = new StringBuilder(30 * strings.size()); // rough guess for capacity!
        for (Object string : strings) {
            builder.append(string).append(delimiter);
        }
        builder.setLength(builder.length() - delimiter.length());
        return builder.toString();
    }

    /**
     * Dumps the content of a resource into a List<String>, where each element is one line of the resource.
     * @param forClass the class whose resource we should get; if null, will get the default
     * <tt>ClassLoader.getSystemResourceAsStream</tt>.
     * @param path the name of the resource
     * @return a list of lines in the resource
     * @throws IOException if the given resource doesn't exist or can't be properly read
     */
    public static List<String> dumpResource(Class<?> forClass, String path) throws IOException {
        InputStream is = (forClass == null)
                ?  ClassLoader.getSystemResourceAsStream(path)
                : forClass.getResourceAsStream(path);
        if (is == null) {
            throw new FileNotFoundException("For class " + forClass + ": " + path);
        }
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            List<String> ret = new ArrayList<String>();
            String line;
            while ((line=reader.readLine()) != null) {
                ret.add(line);
            }
            return ret;
        } finally {
            is.close();
        }
    }

    @SuppressWarnings("unused") // primarily useful in debuggers
    public static String dumpException(Throwable t) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        t.printStackTrace(printWriter);
        printWriter.flush();
        stringWriter.flush();
        return stringWriter.toString();
    }

    @SuppressWarnings("unused") // primarily useful in debuggers
    public static String[] dumpExceptionAsArray(Throwable t) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        t.printStackTrace(printWriter);
        printWriter.flush();
        stringWriter.flush();
        return stringWriter.toString().split("\\n");
    }
}
