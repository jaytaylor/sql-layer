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

package com.foundationdb.server.service.restdml;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.ais.model.aisb2.NewTableBuilder;
import com.foundationdb.server.error.KeyColumnMismatchException;
import com.foundationdb.server.error.NoSuchColumnException;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.server.types.service.TestTypesRegistry;
import com.foundationdb.util.WrappingByteSource;

import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class PrimaryKeyParserTest {
    private static Index createIndex(int colCount, boolean binary) {
        NewAISBuilder builder = AISBBasedBuilder.create("test",
                                                        MTypesTranslator.INSTANCE);
        String[] colNames = new String[colCount];
        NewTableBuilder table = builder.table("t");
        char colName = 'a';
        for(int i = 0; i < colCount; ++i) {
            colNames[i] = String.valueOf(colName++);
            if (binary) {
                table.colVarBinary(colNames[i], 16, false);
            }
            else {
                table.colInt(colNames[i], false);
            }
        }
        table.pk(colNames);
        return builder.ais(true).getTable("test", "t").getPrimaryKey().getIndex();
    }

    private static void test(String input, Index pk, List<List<Object>> expected) {
        List<List<Object>> actual = PrimaryKeyParser.parsePrimaryKeys(input, pk);
        assertEquals(expected, actual);
    }

    private static List<List<Object>> llist(Object... pks) {
        List<List<Object>> list = new ArrayList<>();
        for(Object s : pks) {
            list.add(asList(s));
        }
        return list;
    }

    private static List<List<Object>> blist(int... ints) {
        byte[] bytes = new byte[ints.length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte)ints[i];
        }
        return asList(asList((Object)new WrappingByteSource(bytes)));
    }

    private static String urlEnc(String s) {
        try {
            return URLEncoder.encode(s, "UTF8");
        } catch(UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void singleColumn() {
        Index pk = createIndex(1, false);
        test("1", pk, llist("1"));
        test("1;2", pk, llist("1", "2"));
        test("-10;10;-100;100", pk, llist("-10", "10", "-100", "100"));
    }

    @Test
    public void multiColumn() {
        Index pk = createIndex(2, false);
        test("10,100", pk, asList(asList((Object)"10", (Object)"100")));
        test("20,200;30,300", pk, asList(asList((Object)"20", (Object)"200"), asList((Object)"30", (Object)"300")));
        pk = createIndex(3, false);
        test("1,2,3;4,5,6;7,8,9", pk, asList(asList((Object)"1", (Object)"2", (Object)"3"), asList((Object)"4", (Object)"5", (Object)"6"), asList((Object)"7", (Object)"8", (Object)"9")));
    }

    @Test
    public void columnNameQualified() {
        Index pk = createIndex(1, false);
        test("a=1", pk, llist("1"));
        test("a=1;a=2;a=10", pk, llist("1", "2", "10"));
        pk = createIndex(3, false);
        test("a=1,b=2,c=3;a=10,b=20,c=30", pk, asList(asList((Object)"1", (Object)"2", (Object)"3"), asList((Object)"10", (Object)"20", (Object)"30")));
    }

    @Test
    public void urlEncoded() {
        Index pk = createIndex(1, false);
        test(urlEnc("zip zap"), pk, llist("zip zap"));
        test(urlEnc("a=b=c=d"), pk, llist("a=b=c=d"));
        test(urlEnc("☃"), pk, llist("☃"));
        test(urlEnc("foo,bar") + ";" + urlEnc("cab;cop"), pk, llist("foo,bar", "cab;cop"));
        pk = createIndex(2, false);
        test("a=" + urlEnc("Acme/Corp") + ",b=" + urlEnc("@example.com"), pk, asList(asList((Object)"Acme/Corp", (Object)"@example.com")));
    }

    @Test
    public void outOfOrderQualified() {
        Index pk = createIndex(3, false);
        test("a=1,b=2,c=3", pk, asList(asList((Object)"1", (Object)"2", (Object)"3")));
        test("c=3,b=2,a=1", pk, asList(asList((Object)"1", (Object)"2", (Object)"3")));
        test("b=2,c=3,a=1", pk, asList(asList((Object)"1", (Object)"2", (Object)"3")));
    }

    @Test
    public void mixedQualifiedSeparatePKs() {
        Index pk = createIndex(1, false);
        test("a=1;2;a=3", pk, llist("1", "2", "3"));
        test("4;a=5;6", pk, llist("4", "5", "6"));
    }

    @Test(expected=KeyColumnMismatchException.class)
    public void underColumn() {
        Index pk = createIndex(2, false);
        test("1", pk, llist("1"));
    }

    @Test(expected=KeyColumnMismatchException.class)
    public void overColumn() {
        Index pk = createIndex(1, false);
        test("1,2", pk, asList(asList((Object)"1", (Object)"2")));
    }

    @Test(expected=KeyColumnMismatchException.class)
    public void columnNotInIndex() {
        Index pk = createIndex(1, false);
        test("z=1", pk, llist("1"));
    }

    @Test(expected=KeyColumnMismatchException.class)
    public void mixedQualifiedSamePK() {
        Index pk = createIndex(2, false);
        test("a=1,2", pk, asList(asList((Object)"1", (Object)"2")));
    }

    @Test
    public void urlEncodedBinary() {
        Index pk = createIndex(1, true);
        test("a=%01%02%03", pk, blist(0x01, 0x02, 0x03));
        // NOTE: This does not work as a URL component because Jetty gets an error
        // setting up the decoded path component before calling our handler.
        test("a=%FF%FE%FD", pk, blist(0xFF, 0xFE, 0xFD));
    }

    @Test
    public void hexBinary() {
        Index pk = createIndex(1, true);
        test("a=hex:010203", pk, blist(0x01, 0x02, 0x03));
        test("a=hex:FFFEFD", pk, blist(0xFF, 0xFE, 0xFD));
    }

    @Test
    public void base64Binary() {
        Index pk = createIndex(1, true);
        test("a=base64:AQIDBA==", pk, blist(0x01, 0x02, 0x03, 0x04));
        // NOTE: This could not actually be a URL component because of the slash.
        test("a=base64://79/A==", pk, blist(0xFF, 0xFE, 0xFD, 0xFC));
    }

}
