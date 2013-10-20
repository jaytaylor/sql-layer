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

package com.foundationdb.util;

import com.foundationdb.server.error.InvalidParameterValueException;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;
import java.util.Map.Entry;
import java.util.jar.JarEntry;

/**
 * String utils.
 */
public abstract class Strings {
    
    public static final String NL = nl();

    public static List<String> entriesToString(Map<?, ?> map) {
        List<String> result = new ArrayList<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet())
            result.add(entry.toString());
        return result;
    }

    private static class ListAppendable implements Appendable {
        private final List<String> list;

        public ListAppendable(List<String> list) {
            this.list = list;
        }

        @Override
        public Appendable append(CharSequence csq) throws IOException {
            list.add(csq.toString());
            return this;
        }

        @Override
        public Appendable append(CharSequence csq, int start, int end) throws IOException {
            return append(csq.subSequence(start, end).toString());
        }

        @Override
        public Appendable append(char c) throws IOException {
            return append(String.valueOf(c));
        }
    }


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
    public static String join(Object... strings) {
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

    public static List<String> stringAndSort(Collection<?> inputs) {
        List<String> results = new ArrayList<>(inputs.size());
        
        for (Object item : inputs) {
            String asString = stringAndSort(item);
            results.add(asString);
        }
        
        Collections.sort(results);
        return results;
    }

    public static List<String> stringAndSort(Map<?,?> inputs) {
        // step 1: get the key-value pairs into a multimap. We need a multimap because multiple keys may have the
        // same toString. For instance, { 1 : "int", 1L : "long" } would become a multimap { "1" : ["int", "long"] }
        Multimap<String,String> multiMap = ArrayListMultimap.create();
        for (Map.Entry<?,?> inputEntry : inputs.entrySet()) {
            String keyString = stringAndSort(inputEntry.getKey());
            String valueString = stringAndSort(inputEntry.getValue());
            multiMap.put(keyString, valueString);
        }
        // step 2: Flatten the multimap into a Map<String,String>, sorting by keys as you go.
        Map<String,String> sortedAndFlattened = new TreeMap<>();
        for (Entry<String,Collection<String>> multiMapEntry : multiMap.asMap().entrySet()) {
            String keyString = multiMapEntry.getKey();
            String valueString = stringAndSort(multiMapEntry.getValue()).toString();
            String duplicate = sortedAndFlattened.put(keyString, valueString);
            assert duplicate == null : duplicate;
        }

        // step 3: Flatten the map into a List<String>
        List<String> results = new ArrayList<>(inputs.size());
        for (Entry<String,String> entry : sortedAndFlattened.entrySet()) {
            results.add(entry.toString());
        }
        return results;
    }

    private static String stringAndSort(Object item) {
        if (item instanceof Collection) {
            return stringAndSort((Collection<?>)item).toString();
        }
        else if (item instanceof Map) {
            return stringAndSort((Map<?,?>)item).toString();
        }
        else {
            return String.valueOf(item);
        }
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
        List<String> result = new ArrayList<>();
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
                readStreamTo(is, new ListAppendable(result), false);
            }
        }
        return result;
    }

    @SuppressWarnings("unused") // primarily useful in debuggers
    public static String[] dumpException(Throwable t) {
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
   
    /** For example, '0' returns 0, 'a' or 'A' returns 10, etc */
    private static int hexCharToInt(char c)
    {
        int lower = c | 32;
        if(lower >= '0' && lower <= '9')
            return lower - '0';
        if(lower >= 'a' && lower <= 'f')
            return 10 + lower - 'a';
        throw new InvalidParameterValueException("Invalid HEX digit: " + c);
    }
    
    
    /** For example, ('2','0') returns 32 */
    private static byte hexCharsToByte(char highChar, char lowChar)
    {
        return (byte)((hexCharToInt(highChar) << 4) + hexCharToInt(lowChar));
    }
    
    public static ByteSource parseHexWithout0x (String st) 
    {
        int odd = st.length() & 0x01;
        int outputLen = (st.length() >> 1) + odd;
        byte ret[] = new byte[outputLen];

        int stIndex;
        if (odd == 0) {
            ret[0] = hexCharsToByte(st.charAt(0), st.charAt(1));
            stIndex = 2;
        } else {
            ret[0] = (byte)hexCharToInt(st.charAt(0));
            stIndex = 1;
        }
        
        // starting from here, all characters should be evenly divided into pair        
        for(int retIndex = 1; retIndex < ret.length; ++retIndex, stIndex += 2) {
            ret[retIndex] = hexCharsToByte(st.charAt(stIndex), st.charAt(stIndex + 1));
        }
        
        return new WrappingByteSource(ret);
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

    public static List<String> readStream(InputStream is) throws IOException {
        List<String> result = new ArrayList<>();
        readStreamTo(is, new ListAppendable(result), false);
        return result;
    }

    public static void readStreamTo(InputStream is, Appendable out, boolean keepNL) throws IOException {
        readerTo(bufferedReader(is), out, keepNL);
    }

    private static void readerTo(BufferedReader reader, Appendable out, boolean keepNL) throws IOException {
        try {
            String line;
            while ((line=reader.readLine()) != null) {
                out.append(line);
                if (keepNL)
                    out.append(NL);
            }
        } finally {
            reader.close();
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
    
    public static String stripr(String input, String suffix) {
        if (input == null || suffix == null)
            return input;
        return input.endsWith(suffix)
                ? input.substring(0, input.length() - suffix.length())
                : input;
    }

    private static final Logger LOG = LoggerFactory.getLogger(Strings.class);

    public static List<String> dumpFile(File file) throws IOException {
        List<String> results = new ArrayList<>();
        readerTo(bufferedReader(new FileInputStream(file)), new ListAppendable(results), false);
        return results;
    }

    public static String dumpFileToString(File file) throws IOException {
        StringBuilder builder = new StringBuilder();
        readerTo(bufferedReader(new FileInputStream(file)), builder, true);
        return builder.toString();
    }
    
    public static List<String> mapToString(Collection<?> collection) {
        // are lambdas here yet?!
        List<String> strings = new ArrayList<>(collection.size());
        for (Object o : collection)
            strings.add(String.valueOf(o));
        return strings;
    }

    public static boolean equalCharsets(Charset one, String two) {
        return one.name().equals(two) || one.equals(Charset.forName(two));
    }

    private static BufferedReader bufferedReader(InputStream is) {
        try {
            return new BufferedReader(new InputStreamReader(is, "UTF-8"));
        } catch(UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static String formatMD5(byte[] md5, boolean toLowerCase) {
        return new String(Hex.encodeHex(md5, toLowerCase));
    }
}
