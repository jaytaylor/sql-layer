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

package com.akiban.server.test.it.qp;

/*
 * This test is inspired by Bug 837706, in which a left join row was not being ordered correctly relative
 * to siblings. The schema for this test uses this grouping:
 *     ancestor
 *         parent
 *             before_child
 *             child
 *             after_child
 * and checks that a parent/child left join row is ordered correctly in the presence of any combination
 * of before_child and after_child rows.
 */

import com.akiban.ais.model.Group;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.Comparison;
import org.junit.Before;
import org.junit.Test;

import static com.akiban.qp.operator.API.FlattenOption.KEEP_PARENT;
import static com.akiban.qp.operator.API.JoinType.LEFT_JOIN;
import static com.akiban.qp.operator.API.*;
import static com.akiban.server.expression.std.Expressions.*;

import static com.akiban.qp.rowtype.RowTypeChecks.checkRowTypeFields;
import static com.akiban.server.types.AkType.*;

public class FlattenLeftJoinIT extends OperatorITBase
{
    @Before
    public void before()
    {
        // Don't call super.before(). This is a different schema from most operator ITs.
        ancestor = createTable(
            "schema", "ancestor",
            "aid int not null primary key",
            "avalue varchar(20)");
        parent = createTable(
            "schema", "parent",
            "pid int not null primary key",
            "aid int",
            "pvalue varchar(20)",
            "grouping foreign key(aid) references ancestor(aid)");
        beforeChild = createTable(
            "schema", "before_child",
            "bid int not null primary key",
            "pid int",
            "bvalue varchar(20)",
            "grouping foreign key(pid) references parent(pid)");
        child = createTable(
            "schema", "child",
            "cid int not null primary key",
            "pid int",
            "cvalue varchar(20)",
            "grouping foreign key(pid) references parent(pid)");
        afterChild = createTable(
            "schema", "after_child",
            "aid int not null primary key",
            "pid int",
            "avalue varchar(20)",
            "grouping foreign key(pid) references parent(pid)");
        schema = new Schema(ais());
        ancestorRowType = schema.userTableRowType(userTable(ancestor));
        parentRowType = schema.userTableRowType(userTable(parent));
        beforeChildRowType = schema.userTableRowType(userTable(beforeChild));
        childRowType = schema.userTableRowType(userTable(child));
        afterChildRowType = schema.userTableRowType(userTable(afterChild));
        parentPidIndexType = indexType(parent, "pid");
        group = group(ancestor);
        db = new NewRow[]{
            // case 1: one row of each type (except child)
            createNewRow(ancestor, 1L, "a1"),
            createNewRow(parent, 11L, 1L, "p1"),
            createNewRow(beforeChild, 111L, 11L, "b1"),
            createNewRow(afterChild, 111L, 11L, "a1"),
            // case 2: no before_child row
            createNewRow(ancestor, 2L, "a2"),
            createNewRow(parent, 22L, 2L, "p2"),
            createNewRow(afterChild, 222L, 22L, "a2"),
            // case 3: no after_child row
            createNewRow(ancestor, 3L, "a3"),
            createNewRow(parent, 33L, 3L, "p3"),
            createNewRow(beforeChild, 333L, 33L, "b3"),
            // case 4: no before_child or after_child row
            createNewRow(ancestor, 4L, "a4"),
            createNewRow(parent, 41L, 4L, "p41"),
            createNewRow(parent, 42L, 4L, "p42"),
        };
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        use(db);
    }

    @Test
    public void testCase1()
    {
        Operator plan =
            flatten_HKeyOrdered(
                select_HKeyOrdered(
                    groupScan_Default(group),
                    ancestorRowType,
                    selectAncestor(1)),
                parentRowType,
                childRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        RowType pcRowType = plan.rowType();
        checkRowTypeFields(pcRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowBase[] expected = new RowBase[]{
            row(ancestorRowType, 1L, "a1"),
            row(parentRowType, 11L, 1L, "p1"),
            row(beforeChildRowType, 111L, 11L, "b1"),
            row(pcRowType, 11L, 1L, "p1", null, null, null),
            row(afterChildRowType, 111L, 11L, "a1"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCase2()
    {
        Operator plan =
            flatten_HKeyOrdered(
                select_HKeyOrdered(
                    groupScan_Default(group),
                    ancestorRowType,
                    selectAncestor(2)),
                parentRowType,
                childRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        RowType pcRowType = plan.rowType();
        checkRowTypeFields(pcRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowBase[] expected = new RowBase[]{
            row(ancestorRowType, 2L, "a2"),
            row(parentRowType, 22L, 2L, "p2"),
            row(pcRowType, 22L, 2L, "p2", null, null, null),
            row(afterChildRowType, 222L, 22L, "a2"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCase3()
    {
        Operator plan =
            flatten_HKeyOrdered(
                select_HKeyOrdered(
                    groupScan_Default(group),
                    ancestorRowType,
                    selectAncestor(3)),
                parentRowType,
                childRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        RowType pcRowType = plan.rowType();
        checkRowTypeFields(pcRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowBase[] expected = new RowBase[]{
            row(ancestorRowType, 3L, "a3"),
            row(parentRowType, 33L, 3L, "p3"),
            row(beforeChildRowType, 333L, 33L, "b3"),
            row(pcRowType, 33L, 3L, "p3", null, null, null),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCase4()
    {
        Operator plan =
            flatten_HKeyOrdered(
                select_HKeyOrdered(
                    groupScan_Default(group),
                    ancestorRowType,
                    selectAncestor(4)),
                parentRowType,
                childRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        RowType pcRowType = plan.rowType();
        checkRowTypeFields(pcRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowBase[] expected = new RowBase[]{
            row(ancestorRowType, 4L, "a4"),
            row(parentRowType, 41L, 4L, "p41"),
            row(pcRowType, 41L, 4L, "p41", null, null, null),
            row(parentRowType, 42L, 4L, "p42"),
            row(pcRowType, 42L, 4L, "p42", null, null, null),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCase1FlattenPB()
    {
        Operator flattenPB =
            flatten_HKeyOrdered(
                select_HKeyOrdered(
                    groupScan_Default(group),
                    ancestorRowType,
                    selectAncestor(1)),
                parentRowType,
                beforeChildRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        Operator plan =
            flatten_HKeyOrdered(
                flattenPB,
                parentRowType,
                childRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        RowType pbRowType = flattenPB.rowType();
        checkRowTypeFields(pbRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowType pcRowType = plan.rowType();
        checkRowTypeFields(pcRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowBase[] expected = new RowBase[]{
            row(ancestorRowType, 1L, "a1"),
            row(parentRowType, 11L, 1L, "p1"),
            row(pbRowType, 11L, 1L, "p1", 111L, 11L, "b1"),
            row(pcRowType, 11L, 1L, "p1", null, null, null),
            row(afterChildRowType, 111L, 11L, "a1"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCase2FlattenPB()
    {
        Operator flattenPB =
            flatten_HKeyOrdered(
                select_HKeyOrdered(
                    groupScan_Default(group),
                    ancestorRowType,
                    selectAncestor(2)),
                parentRowType,
                beforeChildRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        Operator plan =
            flatten_HKeyOrdered(
                flattenPB,
                parentRowType,
                childRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        RowType pbRowType = flattenPB.rowType();
        checkRowTypeFields(pbRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowType pcRowType = plan.rowType();
        checkRowTypeFields(pcRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowBase[] expected = new RowBase[]{
            row(ancestorRowType, 2L, "a2"),
            row(parentRowType, 22L, 2L, "p2"),
            row(pbRowType, 22L, 2L, "p2", null, null, null),
            row(pcRowType, 22L, 2L, "p2", null, null, null),
            row(afterChildRowType, 222L, 22L, "a2"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCase3FlattenPB()
    {
        Operator flattenPB =
            flatten_HKeyOrdered(
                select_HKeyOrdered(
                    groupScan_Default(group),
                    ancestorRowType,
                    selectAncestor(3)),
                parentRowType,
                beforeChildRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        Operator plan =
            flatten_HKeyOrdered(
                flattenPB,
                parentRowType,
                childRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        RowType pbRowType = flattenPB.rowType();
        checkRowTypeFields(pbRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowType pcRowType = plan.rowType();
        checkRowTypeFields(pcRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowBase[] expected = new RowBase[]{
            row(ancestorRowType, 3L, "a3"),
            row(parentRowType, 33L, 3L, "p3"),
            row(pbRowType, 33L, 3L, "p3", 333L, 33L, "b3"),
            row(pcRowType, 33L, 3L, "p3", null, null, null),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCase4FlattenPB()
    {
        Operator flattenPB =
            flatten_HKeyOrdered(
                select_HKeyOrdered(
                    groupScan_Default(group),
                    ancestorRowType,
                    selectAncestor(4)),
                parentRowType,
                beforeChildRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        Operator plan =
            flatten_HKeyOrdered(
                flattenPB,
                parentRowType,
                childRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        RowType pbRowType = flattenPB.rowType();
        checkRowTypeFields(pbRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowType pcRowType = plan.rowType();
        checkRowTypeFields(pcRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowBase[] expected = new RowBase[]{
            row(ancestorRowType, 4L, "a4"),
            row(parentRowType, 41L, 4L, "p41"),
            row(pbRowType, 41L, 4L, "p41", null, null, null),
            row(pcRowType, 41L, 4L, "p41", null, null, null),
            row(parentRowType, 42L, 4L, "p42"),
            row(pbRowType, 42L, 4L, "p42", null, null, null),
            row(pcRowType, 42L, 4L, "p42", null, null, null),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCase1FlattenPA()
    {
        Operator flattenPA =
            flatten_HKeyOrdered(
                select_HKeyOrdered(
                    groupScan_Default(group),
                    ancestorRowType,
                    selectAncestor(1)),
                parentRowType,
                afterChildRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        Operator plan =
            flatten_HKeyOrdered(
                flattenPA,
                parentRowType,
                childRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        RowType paRowType = flattenPA.rowType();
        checkRowTypeFields(paRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowType pcRowType = plan.rowType();
        checkRowTypeFields(pcRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowBase[] expected = new RowBase[]{
            row(ancestorRowType, 1L, "a1"),
            row(parentRowType, 11L, 1L, "p1"),
            row(beforeChildRowType, 111L, 11L, "b1"),
            row(pcRowType, 11L, 1L, "p1", null, null, null),
            row(paRowType, 11L, 1L, "p1", 111L, 11L, "a1"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCase2FlattenPA()
    {
        Operator flattenPA =
            flatten_HKeyOrdered(
                select_HKeyOrdered(
                    groupScan_Default(group),
                    ancestorRowType,
                    selectAncestor(2)),
                parentRowType,
                afterChildRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        Operator plan =
            flatten_HKeyOrdered(
                flattenPA,
                parentRowType,
                childRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        RowType paRowType = flattenPA.rowType();
        checkRowTypeFields(paRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowType pcRowType = plan.rowType();
        checkRowTypeFields(pcRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowBase[] expected = new RowBase[]{
            row(ancestorRowType, 2L, "a2"),
            row(parentRowType, 22L, 2L, "p2"),
            row(pcRowType, 22L, 2L, "p2", null, null, null),
            row(paRowType, 22L, 2L, "p2", 222L, 22L, "a2"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCase3FlattenPA()
    {
        Operator flattenPA =
            flatten_HKeyOrdered(
                select_HKeyOrdered(
                    groupScan_Default(group),
                    ancestorRowType,
                    selectAncestor(3)),
                parentRowType,
                afterChildRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        Operator plan =
            flatten_HKeyOrdered(
                flattenPA,
                parentRowType,
                childRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        RowType paRowType = flattenPA.rowType();
        checkRowTypeFields(paRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowType pcRowType = plan.rowType();
        checkRowTypeFields(pcRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowBase[] expected = new RowBase[]{
            row(ancestorRowType, 3L, "a3"),
            row(parentRowType, 33L, 3L, "p3"),
            row(beforeChildRowType, 333L, 33L, "b3"),
            row(pcRowType, 33L, 3L, "p3", null, null, null),
            row(paRowType, 33L, 3L, "p3", null, null, null),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCase4FlattenPA()
    {
        Operator flattenPA =
            flatten_HKeyOrdered(
                select_HKeyOrdered(
                    groupScan_Default(group),
                    ancestorRowType,
                    selectAncestor(4)),
                parentRowType,
                afterChildRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        Operator plan =
            flatten_HKeyOrdered(
                flattenPA,
                parentRowType,
                childRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        RowType paRowType = flattenPA.rowType();
        checkRowTypeFields(paRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowType pcRowType = plan.rowType();
        checkRowTypeFields(pcRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowBase[] expected = new RowBase[]{
            row(ancestorRowType, 4L, "a4"),
            row(parentRowType, 41L, 4L, "p41"),
            row(pcRowType, 41L, 4L, "p41", null, null, null),
            row(paRowType, 41L, 4L, "p41", null, null, null),
            row(parentRowType, 42L, 4L, "p42"),
            row(pcRowType, 42L, 4L, "p42", null, null, null),
            row(paRowType, 42L, 4L, "p42", null, null, null),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    // After the original fix to bug 837706 was committed, the XXX1 query, using a large data set, produced failures.
    // The problem was that an index scan produced only partially hkey-ordered input to the flatten. It was hkey
    // ordered under the parent type, but the parent rows themselves were in index order.
    @Test
    public void testNotCompletelyHKeyOrdered()
    {
        Operator plan =
            flatten_HKeyOrdered(
                branchLookup_Default(
                    indexScan_Default(
                        parentPidIndexType,
                        true),
                    group,
                    parentPidIndexType,
                    parentRowType,
                    InputPreservationOption.DISCARD_INPUT),
                parentRowType,
                childRowType,
                LEFT_JOIN,
                KEEP_PARENT);
        RowType pcRowType = plan.rowType();
        checkRowTypeFields(pcRowType, INT, INT, VARCHAR, INT, INT, VARCHAR);
        RowBase[] expected = new RowBase[]{
            row(parentRowType, 42L, 4L, "p42"),
            row(pcRowType, 42L, 4L, "p42", null, null, null),
            row(parentRowType, 41L, 4L, "p41"),
            row(pcRowType, 41L, 4L, "p41", null, null, null),
            row(parentRowType, 33L, 3L, "p3"),
            row(beforeChildRowType, 333L, 33L, "b3"),
            row(pcRowType, 33L, 3L, "p3", null, null, null),
            row(parentRowType, 22L, 2L, "p2"),
            row(pcRowType, 22L, 2L, "p2", null, null, null),
            row(afterChildRowType, 222L, 22L, "a2"),
            row(parentRowType, 11L, 1L, "p1"),
            row(beforeChildRowType, 111L, 11L, "b1"),
            row(pcRowType, 11L, 1L, "p1", null, null, null),
            row(afterChildRowType, 111L, 11L, "a1"),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    // For use by this class

    private Expression selectAncestor(long aid)
    {
        return compare(field(ancestorRowType, 0), Comparison.EQ, literal(aid));
    }

    // Object state

    private int ancestor;
    private int parent;
    private int beforeChild;
    private int child;
    private int afterChild;
    private UserTableRowType ancestorRowType;
    private UserTableRowType parentRowType;
    private UserTableRowType beforeChildRowType;
    private UserTableRowType childRowType;
    private UserTableRowType afterChildRowType;
    private IndexRowType parentPidIndexType;
    private Group group;
}
