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
import static junit.framework.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.akiban.ais.model.TableName;

public class GroupingTest {
    private final static String SCHEMA = "test_schema";
    private static final GroupingVisitor<List<String>> VISITOR = new ReportingVisitor();
    private static final GroupingVisitor<List<String>> SHORT_CIRCUIT_VISITOR = new ShortCircuitVisitor();

    private static class ReportingVisitor implements GroupingVisitor<List<String>> {
        private final List<String> results = new ArrayList<String>();
        private String seenSchema;
        private int depth = 0;

        protected void say(String message) {
            results.add( (results.size()+1) + ". " + message);
        }

        @Override
        public void start(String defaultSchema) {
            results.clear();
            say("starting: " + defaultSchema);
            seenSchema = defaultSchema;
        }

        @Override
        public void visitGroup(Group group, TableName rootTable) {
            say("group: name " + group.getGroupName().getTableName() + " using table " + rootTable.escaped(seenSchema));
        }

        @Override
        public void finishGroup() {
            say("group finished");
        }

        @Override
        public void visitChild(TableName parentName, List<String> parentColumns, TableName childName, List<String> childColumns) {
            say("join " + childName.escaped(seenSchema) + childColumns
                    + " references " + parentName.escaped(seenSchema) + parentColumns);
        }

        @Override
        public boolean startVisitingChildren() {
            say("entering depth " + (++depth));
            return true;
        }

        @Override
        public void finishVisitingChildren() {
            say("returning to depth " + (--depth));
        }

        @Override
        public List<String> end() {
            say("done");
            return results;
        }
    }

    private static class ShortCircuitVisitor extends ReportingVisitor {
        @Override
        public boolean startVisitingChildren() {
            say("returning false from startVisitingChildren()");
            return false;
        }
    }

    @Test
    public void testTraverseCOIA() throws Exception {
        List<String> actuals = getGrouping().traverse(VISITOR);
        List<String> expecteds = Arrays.asList(
                "1. starting: " + SCHEMA,
                "2. group: name group_01 using table customers",
                "3. entering depth 1",
                "4. join orders[cid] references customers[id]",
                "5. entering depth 2",
                "6. join items[oid, oc2] references orders[id, order_col_2]",
                "7. returning to depth 1",
                "8. join addresses[customer_id] references customers[id]",
                "9. returning to depth 0",
                "10. group finished",
                "11. done"
        );

        assertEquals("results", expecteds.toString(), actuals.toString());
    }

    @Test
    public void testTraverseCOIAShortCircuting() throws Exception {
        List<String> actuals = getGrouping().traverse(SHORT_CIRCUIT_VISITOR);
        List<String> expecteds = Arrays.asList(
                "1. starting: " + SCHEMA,
                "2. group: name group_01 using table customers",
                "3. returning false from startVisitingChildren()",
                "4. group finished",
                "5. done"
        );

        assertEquals("results", expecteds.toString(), actuals.toString());
    }

    @Test
    public void testTraverseOneTableGroup() {
        GroupsBuilder builder = new GroupsBuilder(SCHEMA);
        builder.rootTable(SCHEMA, "customers", "group_01");

        List<String> actuals = builder.getGrouping().checkIntegrity().traverse(VISITOR);
        List<String> expecteds = Arrays.asList(
                "1. starting: " + SCHEMA,
                "2. group: name group_01 using table customers",
                "3. group finished",
                "4. done"
        );

        assertEquals("results", expecteds.toString(), actuals.toString());
    }

    @Test
    public void moveRootToGroup() {
        GroupsBuilder builder = GroupsBuilder.from(getGrouping());

        builder.rootTable(SCHEMA, "managers", "group_02");
        builder.joinTables(SCHEMA, "managers", SCHEMA, "sales_reps").column("id", "manager");
        Grouping actual = builder.getGrouping();
        List<String> parentCols = Arrays.asList("rep_id");
        List<String> childCols = Arrays.asList("sales_rep_id");
        actual.moveChild(SCHEMA, "customers", SCHEMA, "sales_reps", childCols, parentCols);
        actual.checkIntegrity();

        builder = new GroupsBuilder(SCHEMA);
        builder.rootTable(SCHEMA, "managers", "group_02");
        builder.joinTables(SCHEMA, "managers", SCHEMA, "sales_reps").column("id", "manager");
        builder.joinTables(SCHEMA, "sales_reps", SCHEMA, "customers").column("rep_id", "sales_rep_id");
        builder.joinTables(SCHEMA, "customers", SCHEMA, "orders").column("id", "cid");
        builder.joinTables(SCHEMA, "orders", SCHEMA, "items").column("id", "oid").column("order_col_2", "oc2");
        builder.joinTables(SCHEMA, "customers", SCHEMA, "addresses").column("id", "customer_id");
        Grouping expected = builder.getGrouping().checkIntegrity();

        assertEquals("moved group", expected.toString(), actual.toString());
    }

    @Test
    public void moveChildUpHierarchy() {
        Grouping actual = getGrouping();
        List<String> parentCols = Arrays.asList("id");
        List<String> childCols = Arrays.asList("customer_id");
        actual.moveChild(SCHEMA, "items", SCHEMA, "customers", childCols, parentCols);
        actual.checkIntegrity();

        GroupsBuilder builder = new GroupsBuilder(SCHEMA);
        builder.rootTable(SCHEMA, "customers", "group_01");
        builder.joinTables(SCHEMA, "customers", SCHEMA, "orders").column("id", "cid");
        builder.joinTables(SCHEMA, "customers", SCHEMA, "addresses").column("id", "customer_id");
        builder.joinTables(SCHEMA, "customers", SCHEMA, "items").column("id", "customer_id");
        Grouping expected = builder.getGrouping().checkIntegrity();

        assertEquals("moved group", expected.toString(), actual.toString());
    }

    @Test(expected=IllegalStateException.class)
    public void moveChildDownHierarchy() {

        Grouping actual = null;
        try {
            actual = getGrouping();
        }
        catch (Throwable t) {
            fail("unexpected error: " + t);
        }
        List<String> parentCols = Arrays.asList("id");
        List<String> childCols = Arrays.asList("order_id");
        actual.moveChild(SCHEMA, "customers", SCHEMA, "orders", childCols, parentCols);
    }

    @Test
    public void promoteChild() {
        Grouping actual = getGrouping();
        Group newGroup = actual.newGroupFromChild(SCHEMA, "orders", "group_beta");
        assertEquals("group name", "group_beta", newGroup.getGroupName().getTableName());
        actual.checkIntegrity();

        GroupsBuilder builder = new GroupsBuilder(SCHEMA);
        builder.rootTable(SCHEMA, "customers", "group_01");
        builder.joinTables(SCHEMA, "customers", SCHEMA, "addresses").column("id", "customer_id");
        builder.rootTable(SCHEMA, "orders", "group_beta");
        builder.joinTables(SCHEMA, "orders", SCHEMA, "items").column("id", "oid").column("order_col_2", "oc2");
        Grouping expected = builder.getGrouping().checkIntegrity();

        assertEquals("new group", expected.toString(), actual.toString());
    }

    @Test
    public void promoteAlreadyRoot() {
        Grouping actual = getGrouping();

        Group newGroup = actual.newGroupFromChild(SCHEMA, "customers", "the_new_group_name");
        assertEquals("group name", "the_new_group_name", newGroup.getGroupName().getTableName());
        actual.checkIntegrity();

        GroupsBuilder builder = new GroupsBuilder(SCHEMA);
        builder.rootTable(SCHEMA, "customers", "the_new_group_name");
        builder.joinTables(SCHEMA, "customers", SCHEMA, "orders").column("id", "cid");
        builder.joinTables(SCHEMA, "orders", SCHEMA, "items").column("id", "oid").column("order_col_2", "oc2");
        builder.joinTables(SCHEMA, "customers", SCHEMA, "addresses").column("id", "customer_id");
        Grouping expected = builder.getGrouping();
        assertEquals("moved group", expected.toString(), actual.toString());
    }

    private static Grouping getGrouping() {
        GroupsBuilder builder = new GroupsBuilder(SCHEMA);
        builder.rootTable(SCHEMA, "customers", "group_01");
        builder.joinTables(SCHEMA, "customers", SCHEMA, "orders").column("id", "cid");
        builder.joinTables(SCHEMA, "orders", SCHEMA, "items").column("id", "oid").column("order_col_2", "oc2");
        builder.joinTables(SCHEMA, "customers", SCHEMA, "addresses").column("id", "customer_id");

        return builder.getGrouping().checkIntegrity();
    }
}
