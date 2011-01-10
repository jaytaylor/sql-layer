package com.akiban.ais.ddl;

import static junit.framework.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.akiban.ais.ddl.DDLGroupingConverter.FFResult;
import com.akiban.util.Strings;

public final class DDLGroupingConverterTest {

    @Test
    public void testFastForward() throws Exception {
        StringReader reader = new StringReader("create table foo bar CREATE table hi");

        assertEquals("first FF", FFResult.withCreate(""), DDLGroupingConverter.fastForward(reader));
        readIn("create table", reader);
        assertEquals("second FF", FFResult.withCreate(" foo bar "), DDLGroupingConverter.fastForward(reader));
        readIn("create table", reader);
        assertEquals("third FF", FFResult.noCreate(" hi"), DDLGroupingConverter.fastForward(reader));
        assertEquals("third FF", null, DDLGroupingConverter.fastForward(reader));
    }

    @Test
    public void testNormalConversion() throws Exception {
        testConversion("DDLConverter_test_ddl.txt", "DDLConverter_test_ddl_out.txt");
    }

    private static void testConversion(String origResourceName, String convertedResourceName) throws Exception {
        StringWriter writer = new StringWriter();
        Reader originalIn = new InputStreamReader(DDLGroupingConverterTest.class.getResourceAsStream(origResourceName));
        try {
            DDLGroupingConverter.convert(originalIn, writer);
            writer.flush();
            assertEquals("output", readResource(convertedResourceName), writer.toString());
        } finally {
            originalIn.close();
        }
    }

    private static void readIn(String expected, Reader reader) throws IOException {
        char[] actualChars = new char[expected.length()];
        assertEquals("chars read", expected.length(), reader.read(actualChars));
        assertEquals("reader message", list(expected.toCharArray()), list(actualChars));
    }

    private static List<Character> list(char[] array) {
        ArrayList<Character> ret = new ArrayList<Character>(array.length);
        for (char c : array) {
            ret.add(Character.toLowerCase(c));
        }
        return ret;
    }

    private static String readResource(String name) throws IOException{
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(DDLGroupingConverterTest.class.getResourceAsStream(name))
        );
        try {
            List<String> ret = new ArrayList<String>();
            String line;
            while (null != (line = reader.readLine())) {
                ret.add(line);
            }
            return Strings.join(ret);
        } finally {
            reader.close();
        }
    }
}
