package com.akiban.ais.model.staticgrouping;

import org.junit.Test;

import java.io.*;

import static junit.framework.Assert.*;

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
        expectedString.getBuffer().setLength( expectedString.toString().length() - 1 );
        assertEquals(expectedString.toString(), grouping.traverse(VISITOR));
    }
}
