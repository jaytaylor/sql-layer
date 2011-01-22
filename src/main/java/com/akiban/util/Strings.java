package com.akiban.util;

import java.io.*;
import java.util.ArrayList;
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

    public static List<String> dumpResource(Class<?> forClass, String path) throws IOException {
        InputStream is = forClass.getResourceAsStream(path);
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
}
