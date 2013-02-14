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

package com.akiban.server.service.restdml;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.Types;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.KeyColumnMismatchException;
import com.akiban.server.error.NoSuchColumnException;

import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class PrimaryKeyParserTest {
    private static Index createIndex(int colCount) {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        UserTable table = UserTable.create(ais, "test", "t", 0);
        Index pk = TableIndex.create(ais, table, Index.PRIMARY_KEY_CONSTRAINT, 0, true, Index.PRIMARY_KEY_CONSTRAINT);
        char colName = 'a';
        for(int i = 0; i < colCount; ++i) {
            Column c = Column.create(table, String.valueOf(colName++), i, Types.INT, false, null, null, null, null);
            IndexColumn.create(pk, c, i, true, null);
        }
        return pk;
    }

    private static void test(String input, Index pk, List<List<String>> expected) {
        List<List<String>> actual = PrimaryKeyParser.parsePrimaryKeys(input, pk);
        assertEquals(expected, actual);
    }

    private static List<List<String>> llist(String... pks) {
        List<List<String>> list = new ArrayList<>();
        for(String s : pks) {
            list.add(asList(s));
        }
        return list;
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
        Index pk = createIndex(1);
        test("1", pk, llist("1"));
        test("1;2", pk, llist("1", "2"));
        test("-10;10;-100;100", pk, llist("-10", "10", "-100", "100"));
    }

    @Test
    public void multiColumn() {
        Index pk = createIndex(2);
        test("10,100", pk, asList(asList("10", "100")));
        test("20,200;30,300", pk, asList(asList("20", "200"), asList("30", "300")));
        pk = createIndex(3);
        test("1,2,3;4,5,6;7,8,9", pk, asList(asList("1", "2", "3"), asList("4", "5", "6"), asList("7", "8", "9")));
    }

    @Test
    public void columnNameQualified() {
        Index pk = createIndex(1);
        test("a=1", pk, llist("1"));
        test("a=1;a=2;a=10", pk, llist("1", "2", "10"));
        pk = createIndex(3);
        test("a=1,b=2,c=3;a=10,b=20,c=30", pk, asList(asList("1", "2", "3"), asList("10", "20", "30")));
    }

    @Test
    public void urlEncoded() {
        Index pk = createIndex(1);
        test(urlEnc("zip zap"), pk, llist("zip zap"));
        test(urlEnc("a=b=c=d"), pk, llist("a=b=c=d"));
        test(urlEnc("☃"), pk, llist("☃"));
        test(urlEnc("foo,bar") + ";" + urlEnc("cab;cop"), pk, llist("foo,bar", "cab;cop"));
        pk = createIndex(2);
        test("a=" + urlEnc("Acme/Corp") + ",b=" + urlEnc("@example.com"), pk, asList(asList("Acme/Corp", "@example.com")));
    }

    @Test
    public void outOfOrderQualified() {
        Index pk = createIndex(3);
        test("a=1,b=2,c=3", pk, asList(asList("1", "2", "3")));
        test("c=3,b=2,a=1", pk, asList(asList("1", "2", "3")));
        test("b=2,c=3,a=1", pk, asList(asList("1", "2", "3")));
    }

    @Test
    public void mixedQualifiedSeparatePKs() {
        Index pk = createIndex(1);
        test("a=1;2;a=3", pk, llist("1", "2", "3"));
        test("4;a=5;6", pk, llist("4", "5", "6"));
    }

    @Test(expected=KeyColumnMismatchException.class)
    public void underColumn() {
        Index pk = createIndex(2);
        test("1", pk, llist("1"));
    }

    @Test(expected=KeyColumnMismatchException.class)
    public void overColumn() {
        Index pk = createIndex(1);
        test("1,2", pk, asList(asList("1", "2")));
    }

    @Test(expected=KeyColumnMismatchException.class)
    public void columnNotInIndex() {
        Index pk = createIndex(1);
        test("z=1", pk, llist("1"));
    }

    @Test(expected=KeyColumnMismatchException.class)
    public void mixedQualifiedSamePK() {
        Index pk = createIndex(2);
        test("a=1,2", pk, asList(asList("1", "2")));
    }
}
