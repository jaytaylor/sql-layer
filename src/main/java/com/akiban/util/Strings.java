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

import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.jar.JarEntry;

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
    public static String join(Collection<?> strings) {
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
    public static String join(Collection<?> strings, String delimiter) {
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
        return readStream(is);
    }

    public static List<String> dumpURLs(Enumeration<URL> urls) throws IOException {
        List<String> result = new ArrayList<String>();
        while (urls.hasMoreElements()) {
            URL next = urls.nextElement();
            LOG.debug("reading URL: {}", next);
            boolean readAsStream = true;
            if ("jar".equals(next.getProtocol())) {
                JarURLConnection connection = (JarURLConnection)next.openConnection();
                if (connection.getJarEntry().isDirectory()) {
                    readJarConnectionTo(connection, result);
                    readAsStream = false;
                }
            }
            if (readAsStream) {
                InputStream is = next.openStream();
                readStreamTo(is, result);
            }
        }
        return result;
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

    public static String hex(byte[] bytes, int start, int length) {
        ArgumentValidation.isGTE("start", start, 0);
        ArgumentValidation.isGTE("length", length, 0);

        StringBuilder sb = new StringBuilder("0x");
        Formatter formatter = new Formatter(sb, Locale.US);
        for (int i=start; i < start+length; ++i) {
            formatter.format("%02X", bytes[i]);
            if ((i-start) % 2 == 1) {
                sb.append(' ');
            }
        }
        return sb.toString().trim();
    }

    public static String hex(ByteSource byteSource) {
        return hex(byteSource.byteArray(), byteSource.byteArrayOffset(), byteSource.byteArrayLength());
    }

    public static ByteSource parseHex(String string) {
        if (!string.startsWith("0x")) {
            throw new RuntimeException("not a hex string");
        }

        byte[] ret = new byte[ (string.length()-2) / 2 ];

        int resultIndex = 0;
        for (int strIndex=2; strIndex < string.length(); ++strIndex) {
            final char strChar = string.charAt(strIndex);
            if (!Character.isWhitespace(strChar)) {
                int high = (Character.digit(strChar, 16)) << 4;
                char lowChar = string.charAt(++strIndex);
                int low = (Character.digit(lowChar, 16));
                ret[resultIndex++] = (byte) (low + high);
            }
        }

        return new WrappingByteSource().wrap(ret, 0, resultIndex);
    }

    private static List<String> readStream(InputStream is) throws IOException {
        List<String> result = new ArrayList<String>();
        readStreamTo(is, result);
        return result;
    }

    private static void readStreamTo(InputStream is, List<String> outList) throws IOException {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line=reader.readLine()) != null) {
                outList.add(line);
            }
        } finally {
            is.close();
        }
    }

    private static void readJarConnectionTo(JarURLConnection connection, List<String> result) throws IOException {
        assert connection.getJarEntry().isDirectory() : "not a dir: " + connection.getJarEntry();
        // put into entries only the children of the connection's entry, and trim off the entry prefix
        String base = connection.getEntryName();
        Enumeration<JarEntry> enumeration = connection.getJarFile().entries();
        while (enumeration.hasMoreElements()) {
            JarEntry entry = enumeration.nextElement();
            if (entry.getName().startsWith(base))
                result.add(entry.getName().substring(base.length()));
        }
    }
    
    public static <T> String toString(Multimap<T,?> map) {
        StringBuilder sb = new StringBuilder();
        for (Iterator<T> keysIter = map.keySet().iterator(); keysIter.hasNext(); ) {
            T key = keysIter.next();
            sb.append(key).append(" => ");
            for (Iterator<?> valsIter = map.get(key).iterator(); valsIter.hasNext(); ) {
                sb.append(valsIter.next());
                if (valsIter.hasNext())
                    sb.append(", ");
            }
            if (keysIter.hasNext())
                sb.append(nl());
        }
        return sb.toString();
    }

    private static final Logger LOG = LoggerFactory.getLogger(Strings.class);
}
