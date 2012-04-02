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

package com.akiban.ais.model.staticgrouping;

import static junit.framework.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.junit.Test;

public class VisitToStringTest {
    private final static String SCHEMA = "test_schema";
    private final static VisitToString VISITOR = VisitToString.getInstance();

    @Test
    public void oneGroup() throws IOException {
        GroupsBuilder builder = new GroupsBuilder(SCHEMA);
        builder.rootTable(SCHEMA, "customers", "customers");
        builder.joinTables(SCHEMA, "customers", SCHEMA, "orders").column("id", "cid");
        builder.joinTables(SCHEMA, "orders", SCHEMA, "items").column("id", "oid").column("order_col_2", "oc2");
        builder.joinTables(SCHEMA, "customers", SCHEMA, "addresses").column("id", "customer_id");
        builder.joinTables(SCHEMA, "orders", SCHEMA, "shipments").column("id", "o_id");

        test("expected_VisitToStringTest_oneGroup.txt", builder.getGrouping());
    }

    @Test
    public void oneTableGroup() throws IOException {
        GroupsBuilder builder = new GroupsBuilder(SCHEMA);
        builder.rootTable(SCHEMA, "customers", "group_01");

        test("expected_VisitToStringTest_oneTableGroup.txt", builder.getGrouping());
    }

    @Test
    public void oneGroupComplexNaming() throws IOException {
        GroupsBuilder builder = new GroupsBuilder("test_schema_whatever");
        builder.rootTable(SCHEMA, "customers", "group_01");
        builder.joinTables(SCHEMA, "customers", SCHEMA, "or`ders").column("id", "cid");
        builder.joinTables(SCHEMA, "or`ders", SCHEMA, "items").column("id", "oid").column("order`col`2", "oc2");
        builder.joinTables(SCHEMA, "customers", SCHEMA, "addresses").column("id", "customer_id");

        test("expected_VisitToStringTest_oneGroupComplexNaming.txt", builder.getGrouping());
    }

    @Test
    public void twoGroups() throws IOException {
        GroupsBuilder builder = new GroupsBuilder(SCHEMA);

        builder.rootTable(SCHEMA, "customers", "group_01");
        builder.joinTables(SCHEMA, "customers", SCHEMA, "orders").column("id", "cid");
        builder.joinTables(SCHEMA, "orders", SCHEMA, "items").column("id", "oid").column("order_col_2", "oc2");

        builder.rootTable(SCHEMA, "addresses", "group_beta");
        builder.joinTables(SCHEMA, "addresses", SCHEMA, "forwarding").column("id", "current_address");

        test("expected_VisitToStringTest_twoGroups.txt", builder.getGrouping());
    }


    private static void test(String expectedFileName, Grouping grouping) throws IOException {
        StringWriter expectedString = new StringWriter();
        PrintWriter writer = new PrintWriter(expectedString);
        InputStream is = VisitToStringTest.class.getResourceAsStream(expectedFileName);
        if (is == null) {
            throw new IOException("couldn't find resource: " + expectedFileName);
        }
        try {
            BufferedReader buffered = new BufferedReader( new InputStreamReader(is) );
            String line;
            while (null != (line = buffered.readLine())) {
                writer.println(line);
            }
        }
        finally {
            is.close();
        }

        // lop off the last newline
        expectedString.getBuffer().setLength( expectedString.getBuffer().length() - VisitToString.NL.length() );
        assertEquals(expectedString.toString(), grouping.traverse(VISITOR));
    }
}
