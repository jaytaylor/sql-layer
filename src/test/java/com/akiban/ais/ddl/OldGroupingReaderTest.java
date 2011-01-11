package com.akiban.ais.ddl;

import static junit.framework.Assert.assertEquals;

import org.junit.Test;

import com.akiban.ais.model.staticgrouping.Grouping;
import com.akiban.ais.model.staticgrouping.GroupsBuilder;

public final class OldGroupingReaderTest {

    @Test
    public void testOTPG() {
        GroupsBuilder builder = new GroupsBuilder("scm");
        builder.rootTable("scm", "one", "group_one");
        builder.rootTable("scm", "two", "group_two");
        test(builder, "/* schema scm; group group_one { table one }; group group_two { table two }; */ blah blah");
    }

    @Test
    public void testCOIA() {
        GroupsBuilder builder = new GroupsBuilder("scm");
        builder.rootTable("scm", "customers", "coi");
        builder.joinTables("scm", "customers", "scm", "orders").column("cid", "cid");
        builder.joinTables("scm", "orders", "scm", "items").column("oid", "oid");
        test(builder, "/* schema scm; group coi { table customers { table orders(cid) {table items(oid) } } };  */ blah blah");
    }
    
    private static void test(GroupsBuilder expected, String input) {
        try {
            Grouping actual = (new OldGroupingReader()).readString(input);
            assertEquals(expected.getGrouping().toString(), actual.toString() );
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
