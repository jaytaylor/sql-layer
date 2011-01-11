package com.akiban.util;

import java.io.*;
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
    public static String join(List<String> strings) {
        return join(strings, nl());
    }

    /**
     * Joins the given Strings into a single String with the given delimiter. The last String in the list will
     * not have the delimiter appended. If the list is empty, this returns an empty string.
     * @param strings a list of strings. May not be null.
     * @param delimiter the delimiter between strings; this will be inserted <tt>(strings.size() - 1)</tt> times.
     * May not be null.
     * @return the joined string
     */
    public static String join(List<String> strings, String delimiter) {
        if (strings.size() == 0) {
            return "";
        }
        StringBuilder builder = new StringBuilder(30 * strings.size()); // rough guess for capacity!
        for (String string : strings) {
            builder.append(string).append(delimiter);
        }
        builder.setLength(builder.length() - delimiter.length());
        return builder.toString();
    }

    public static String readResource(String resourceName, Class<?> forClass) throws IOException {
        InputStream stream = forClass.getResourceAsStream(resourceName);
        if (stream == null) {
            throw new FileNotFoundException(resourceName);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        try {
            StringBuilder builder = new StringBuilder();
            final String NL = nl();
            for(String line=reader.readLine(); line != null; line=reader.readLine()) {
                builder.append(line).append(NL);
            }
            return builder.toString();
        } finally {
            reader.close();
        }
    }

    public static String readResource(String resourceName) throws IOException {
        return readResource(resourceName, Strings.class);
    }
}
