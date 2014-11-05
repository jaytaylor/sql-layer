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

import com.google.common.io.BaseEncoding;
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
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.Map.Entry;
import java.util.jar.JarEntry;

/**
 * String utils.
 */
public abstract class Strings {
    
    public static final String NL = nl();

    public static final Set<String> PROTECTED_KEYWORDS = new HashSet<String>();

    static {
        PROTECTED_KEYWORDS.addAll(Arrays.asList( "ADD", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "AS", "AT",
            "AUTHORIZATION", "AVG", "BEGIN", "BETWEEN", "BIT", "BOTH", "BY", "CASCADED", "CASE",
            "CAST", "CHAR", "CHARACTER_LENGTH", "CHAR_LENGTH", "CHECK", "CLOSE", "COLLATE", "COLUMN",
            "COMMIT", "CONNECT", "CONNECTION", "CONSTRAINT", "CONTINUE", "CONVERT", "CORRESPONDING",
            "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
            "CURSOR", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "_DEFAULT", "DELETE", "DESCRIBE",
            "DISCONNECT", "DISTINCT", "DOUBLE", "DROP", "ELSE", "END", "ENDEXEC", "ESCAPE", "EXCEPT",
            "EXEC", "EXECUTE", "EXISTS", "EXTERNAL", "FALSE", "FETCH", "FLOAT", "FOR", "FOREIGN",
            "FROM", "FULL", "FUNCTION", "GET", "GET_CURRENT_CONNECTION", "GLOBAL", "GRANT", "GROUP",
            "GROUP_CONCAT", "HAVING", "HOUR", "IDENTITY", "IMMEDIATE", "IN", "INDEX", "INDICATOR",
            "INNER", "INOUT", "INPUT", "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT",
            "INTERVAL", "INTO", "IS", "JOIN", "LEADING", "LEFT", "LIKE", "LIMIT", "LOWER", "MATCH",
            "MAX", "MIN", "MINUTE", "NATIONAL", "NATURAL", "NCHAR", "NVARCHAR", "NEXT", "NO", "NONE",
            "NOT", "NULL", "NULLIF", "NUMERIC", "OCTET_LENGTH", "OF", "ON", "ONLY", "OPEN", "OR",
            "ORDER", "OUT", "OUTER", "OUTPUT", "OVERLAPS", "PARTITION", "PREPARE", "PRIMARY",
            "PROCEDURE", "PUBLIC", "REAL", "REFERENCES", "RESTRICT", "RETURNING", "REVOKE", "RIGHT",
            "ROLLBACK", "ROWS", "SCHEMA", "SCROLL", "SECOND", "SELECT", "SESSION_USER", "SET",
            "SMALLINT", "SOME", "SQL", "SQLCODE", "SQLERROR", "SQLSTATE", "STRAIGHT_JOIN",
            "SUBSTRING", "SUM", "SYSTEM_USER", "TABLE", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO",
            "TRAILING", "TRANSLATE", "TRANSLATION", "TRUE", "UNION", "UNIQUE", "UNKNOWN", "UPDATE",
            "UPPER", "USER", "USING", "VALUES", "VARCHAR", "VARYING", "WHENEVER", "WHERE", "WITH",
            "YEAR", "BOOLEAN", "CALL", "CURRENT_ROLE", "CURRENT_SCHEMA", "EXPLAIN", "GROUPING",
            "LTRIM", "RTRIM", "TRIM", "SUBSTR", "XML", "XMLPARSE", "XMLSERIALIZE", "XMLEXISTS",
            "XMLQUERY", "Z_ORDER_LAT_LON" ));
    };



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

    public static String repeatString(String str, int count) {
        StringBuilder sb = new StringBuilder(str.length() * count);
        while (count-- > 0) {
            sb.append(str);
        }
        return sb.toString();
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
    
    public static String toOctal(byte[] bytes){
        StringBuilder out = new StringBuilder();
        for (byte b : bytes) {
            out.append(String.format("\\%03o", b));
        }
        return out.toString();
    }

    public static String hex(byte[] bytes) {
        return hex(bytes, 0, bytes.length);
    }

    public static String hex(ByteSource byteSource) {
        return hex(byteSource.byteArray(), byteSource.byteArrayOffset(), byteSource.byteArrayLength());
    }

    public static String hex(byte[] bytes, int start, int length) {
        return BaseEncoding.base16().encode(bytes, start, length);
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
        int si = 0, ri = 0;
        if (odd == 1) {
            ret[ri++] = (byte)hexCharToInt(st.charAt(si++));
        }
        while(ri < ret.length) {
            ret[ri++] = hexCharsToByte(st.charAt(si++), st.charAt(si++));
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
            if(keepNL) {
                CharBuffer buffer = CharBuffer.allocate(1024);
                while(reader.read(buffer) != -1) {
                    buffer.flip();
                    out.append(buffer);
                    buffer.clear();
                }
            } else {
                String line;
                while((line = reader.readLine()) != null) {
                    out.append(line);
                }
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
        BaseEncoding encoder = toLowerCase ? BaseEncoding.base16().lowerCase() : BaseEncoding.base16().upperCase();
        return encoder.encode(md5);
    }

    public static String truncateIfNecessary(String str, int codePointCount) {
        // Try to avoid scanning the string for surrogates, which are rare in the wild.
        int nchars = str.length();
        if (nchars <= codePointCount)
            return str;
        int ncode = str.codePointCount(0, nchars);
        if (ncode <= codePointCount)
            return str;
        if (nchars == ncode)
            return str.substring(0, codePointCount);
        else
            return str.substring(0, str.offsetByCodePoints(0, codePointCount));
    }

    public static String toBase64(byte[] bytes) {
        return toBase64(bytes, 0, bytes.length);
    }

    public static String toBase64(byte[] bytes, int offset, int length) {
        return BaseEncoding.base64().encode(bytes, offset, length);
    }

    public static byte[] fromBase64(CharSequence cs) {
        return BaseEncoding.base64().decode(cs);
    }

    /**
     * Split a period delimited string of identifiers into an array of
     * constituent pieces. Up to {@code maxParts} many identifiers will
     * be returned and non-quoted identifiers will be lower-cased.
     * <p>
     * For example,
     * <pre>
     * (test.t, 1)  => [test]
     * (test.t, 2)  => [test, t]
     * (test.t, 3)  => ["", test, t]
     * (A.B,    2)  => [a, b]
     * ("a.B".t, 2) => [a.B, t]
     * </pre>
     * </p>
     */
    public static String[] parseQualifiedName(String arg, int maxParts) {
        assert maxParts > 0 : maxParts;
        String[] result = new String[maxParts];
        int resIndex = 0;
        char lastQuote = 0;
        int prevEnd = 0;
        for(int i = 0; i < arg.length(); ++i) {
            char c = arg.charAt(i);
            boolean take = false;
            boolean toLower = true;
            if(lastQuote != 0) {
                if(c == lastQuote) {
                    take = true;
                    toLower = false;
                    lastQuote = 0;
                }
            } else if(c == '"' || c == '`') {
                lastQuote = c;
                prevEnd = i + 1;
            } else if(c == '.') {
                take = true;
            }
            if(take) {
                if(prevEnd < i) {
                    result[resIndex++] = consumeIdentifier(arg, prevEnd, i, toLower);
                }
                prevEnd = i + 1;
            }
        }
        if((resIndex < maxParts) && (prevEnd < arg.length())) {
            result[resIndex++] = consumeIdentifier(arg, prevEnd, arg.length(), lastQuote == 0);
        }
        int diff = maxParts - resIndex;
        if(diff > 0) {
            // Shift found and empty fill missing
            System.arraycopy(result, 0, result, maxParts - resIndex, resIndex);
            for(int i = 0; i < diff; ++i) {
                result[i] = "";
            }
        }
        return result;
    }

    public static String quotedIdent(String s, char quote, boolean force) {

        String quoteS = Character.toString(quote);

        if (s.contains(quoteS)){
             s = s.replaceAll( ("[" + quoteS + "]") , quoteS + quoteS );
        }

        if (!force  && (!PROTECTED_KEYWORDS.contains(s.toUpperCase()))
                            && s.matches("[A-Za-z][_A-Za-z0-9$]*") ) {
            return s;
        }
        else {
            return quote + s + quote;
        }
    }

    public static String escapeIdentifier(String s) {
        return String.format("\"%s\"", s.replace("\"", "\"\""));
    }

    //
    // Internal
    //

    private static String consumeIdentifier(String arg, int begin, int end, boolean toLower) {
        String s = arg.substring(begin, end);
        if(toLower) {
            s = s.toLowerCase();
        }
        return s;
    }
}
