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

package com.akiban.util;

import com.akiban.server.error.InvalidParameterValueException;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
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
    private static final int BASE_CHAR = 10 -'a';
    private static final Set<Character> LEGAL_HEX = new HashSet<Character>();
    static
    {
       for (char ch = 'a'; ch <= 'f'; ++ch)
           LEGAL_HEX.add(ch);
       for (char ch = '0'; ch <= '9'; ++ch)
           LEGAL_HEX.add(ch); 
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
        List<String> results = new ArrayList<String>(inputs.size());
        
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
        Map<String,String> sortedAndFlattened = new TreeMap<String,String>();
        for (Entry<String,Collection<String>> multiMapEntry : multiMap.asMap().entrySet()) {
            String keyString = multiMapEntry.getKey();
            String valueString = stringAndSort(multiMapEntry.getValue()).toString();
            String duplicate = sortedAndFlattened.put(keyString, valueString);
            assert duplicate == null : duplicate;
        }

        // step 3: Flatten the map into a List<String>
        List<String> results = new ArrayList<String>(inputs.size());
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
   
    /**
     * @param c: character
     * @return the HEX value of this char
     * @throws InvalidParameterValueException if c is not a valid hex digit
     * 
     * Eg., 'a' would return 10
     */
    private static int getHex (char c)
    {
        if (!LEGAL_HEX.contains(c |= 32))
            throw new InvalidParameterValueException("Invalid HEX digit: " + c);
        
        return c > 'a'
                    ? c + BASE_CHAR
                    : c - '0';
    }
    
    
    /**
     *
     * @param highChar
     * @param lowChar
     * @return a character whose (ASCII) code is equal to the hexadecimal value
     *         of <highChar><lowChar>
     * @throws InvalidParameterValue if either of the two char is not a legal
     *         hex digit
     *
     * Eg., parseByte('2', '0') should return ' ' (space character)
     *
     */
    private static byte getByte (char highChar, char lowChar)
    {
        return (byte)((getHex(highChar) << 4) + getHex(lowChar));
    }
    
    public static ByteSource parseHexWithout0x (String st) 
    {
        double quotient = st.length() / 2.0;
        byte ret[] = new byte[(int)Math.ceil(quotient)];
        int stIndex = 0, retIndex = 0;
        
        // if all the chars in st can be evenly divided into pairs
        if (ret.length == (int)quotient)
            // two first hex digits make a byte
            ret[retIndex++] = getByte(st.charAt(stIndex), st.charAt(++stIndex));
        else // if not
            // only the first one does
            ret[retIndex++] = (byte)getHex(st.charAt(stIndex));
        
        // starting from here, all characters should be evenly divided into pair        
        for (; retIndex < ret.length; ++retIndex)
            ret[retIndex] = getByte(st.charAt(++stIndex), st.charAt(++stIndex));
        
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
    
    public static String stripr(String input, String suffix) {
        if (input == null || suffix == null)
            return input;
        return input.endsWith(suffix)
                ? input.substring(0, input.length() - suffix.length())
                : input;
    }

    private static final Logger LOG = LoggerFactory.getLogger(Strings.class);

    public static List<String> dumpFile(File file) throws IOException {
        List<String> results = new ArrayList<String>();
        FileReader reader = new FileReader(file);
        try {
            BufferedReader buffered = new BufferedReader(reader);
            for (String line; (line=buffered.readLine()) != null; ) {
                results.add(line);
            }
        } finally {
            reader.close();
        }
        return results;
    }
    
    public static List<String> mapToString(Collection<?> collection) {
        // are lambdas here yet?!
        List<String> strings = new ArrayList<String>(collection.size());
        for (Object o : collection)
            strings.add(String.valueOf(o));
        return strings;
    }

    public static boolean equalCharsets(Charset one, String two) {
        return one.name().equals(two) || one.equals(Charset.forName(two));
    }
}
