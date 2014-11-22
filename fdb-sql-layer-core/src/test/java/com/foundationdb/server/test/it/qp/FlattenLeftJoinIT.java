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

package com.foundationdb.server.test.it.qp;

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

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.texpressions.Comparison;

import org.junit.Test;

import static com.foundationdb.qp.operator.API.FlattenOption.KEEP_PARENT;
import static com.foundationdb.qp.operator.API.JoinType.LEFT_JOIN;
import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.*;
import static com.foundationdb.qp.rowtype.RowTypeChecks.checkRowTypeFields;

public class FlattenLeftJoinIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
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
    }

    @Override
    protected void setupPostCreateSchema()
    {
        ancestorRowType = schema.tableRowType(table(ancestor));
        parentRowType = schema.tableRowType(table(parent));
        beforeChildRowType = schema.tableRowType(table(beforeChild));
        childRowType = schema.tableRowType(table(child));
        afterChildRowType = schema.tableRowType(table(afterChild));
        parentPidIndexType = indexType(parent, "pid");
        group = group(ancestor);
        db = new Row[]{
            // case 1: one row of each type (except child)
            row(ancestor, 1L, "a1"),
            row(parent, 11L, 1L, "p1"),
            row(beforeChild, 111L, 11L, "b1"),
            row(afterChild, 111L, 11L, "a1"),
            // case 2: no before_child row
            row(ancestor, 2L, "a2"),
            row(parent, 22L, 2L, "p2"),
            row(afterChild, 222L, 22L, "a2"),
            // case 3: no after_child row
            row(ancestor, 3L, "a3"),
            row(parent, 33L, 3L, "p3"),
            row(beforeChild, 333L, 33L, "b3"),
            // case 4: no before_child or after_child row
            row(ancestor, 4L, "a4"),
            row(parent, 41L, 4L, "p41"),
            row(parent, 42L, 4L, "p42"),
        };
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
    }

    private void checkRowFields (RowType pcRowType) {
        checkRowTypeFields(null, pcRowType, 
                MNumeric.INT.instance(false),
                MNumeric.INT.instance(true),
                MString.VARCHAR.instance(20, true),
                MNumeric.INT.instance(false),
                MNumeric.INT.instance(true),
                MString.VARCHAR.instance(20, true));
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
        checkRowFields(pcRowType);
        Row[] expected = new Row[]{
            row(ancestorRowType, 1L, "a1"),
            row(parentRowType, 11L, 1L, "p1"),
            row(beforeChildRowType, 111L, 11L, "b1"),
            row(pcRowType, 11L, 1L, "p1", null, null, null),
            row(afterChildRowType, 111L, 11L, "a1"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
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
        checkRowFields(pcRowType);
        Row[] expected = new Row[]{
            row(ancestorRowType, 2L, "a2"),
            row(parentRowType, 22L, 2L, "p2"),
            row(pcRowType, 22L, 2L, "p2", null, null, null),
            row(afterChildRowType, 222L, 22L, "a2"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
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
        checkRowFields(pcRowType);
        Row[] expected = new Row[]{
            row(ancestorRowType, 3L, "a3"),
            row(parentRowType, 33L, 3L, "p3"),
            row(beforeChildRowType, 333L, 33L, "b3"),
            row(pcRowType, 33L, 3L, "p3", null, null, null),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
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
        checkRowFields(pcRowType);
        Row[] expected = new Row[]{
            row(ancestorRowType, 4L, "a4"),
            row(parentRowType, 41L, 4L, "p41"),
            row(pcRowType, 41L, 4L, "p41", null, null, null),
            row(parentRowType, 42L, 4L, "p42"),
            row(pcRowType, 42L, 4L, "p42", null, null, null),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
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
        checkRowFields(pbRowType);
        RowType pcRowType = plan.rowType();
        checkRowFields(pcRowType);
        Row[] expected = new Row[]{
            row(ancestorRowType, 1L, "a1"),
            row(parentRowType, 11L, 1L, "p1"),
            row(pbRowType, 11L, 1L, "p1", 111L, 11L, "b1"),
            row(pcRowType, 11L, 1L, "p1", null, null, null),
            row(afterChildRowType, 111L, 11L, "a1"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
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
        checkRowFields(pbRowType);
        RowType pcRowType = plan.rowType();
        checkRowFields(pcRowType);
        Row[] expected = new Row[]{
            row(ancestorRowType, 2L, "a2"),
            row(parentRowType, 22L, 2L, "p2"),
            row(pbRowType, 22L, 2L, "p2", null, null, null),
            row(pcRowType, 22L, 2L, "p2", null, null, null),
            row(afterChildRowType, 222L, 22L, "a2"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
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
        checkRowFields(pbRowType);
        RowType pcRowType = plan.rowType();
        checkRowFields(pcRowType);
        Row[] expected = new Row[]{
            row(ancestorRowType, 3L, "a3"),
            row(parentRowType, 33L, 3L, "p3"),
            row(pbRowType, 33L, 3L, "p3", 333L, 33L, "b3"),
            row(pcRowType, 33L, 3L, "p3", null, null, null),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
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
        checkRowFields(pbRowType);
        RowType pcRowType = plan.rowType();
        checkRowFields(pcRowType);
        Row[] expected = new Row[]{
            row(ancestorRowType, 4L, "a4"),
            row(parentRowType, 41L, 4L, "p41"),
            row(pbRowType, 41L, 4L, "p41", null, null, null),
            row(pcRowType, 41L, 4L, "p41", null, null, null),
            row(parentRowType, 42L, 4L, "p42"),
            row(pbRowType, 42L, 4L, "p42", null, null, null),
            row(pcRowType, 42L, 4L, "p42", null, null, null),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
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
        checkRowFields(paRowType);
        RowType pcRowType = plan.rowType();
        checkRowFields(pcRowType);
        Row[] expected = new Row[]{
            row(ancestorRowType, 1L, "a1"),
            row(parentRowType, 11L, 1L, "p1"),
            row(beforeChildRowType, 111L, 11L, "b1"),
            row(pcRowType, 11L, 1L, "p1", null, null, null),
            row(paRowType, 11L, 1L, "p1", 111L, 11L, "a1"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
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
        checkRowFields(paRowType);
        RowType pcRowType = plan.rowType();
        checkRowFields(pcRowType);
        Row[] expected = new Row[]{
            row(ancestorRowType, 2L, "a2"),
            row(parentRowType, 22L, 2L, "p2"),
            row(pcRowType, 22L, 2L, "p2", null, null, null),
            row(paRowType, 22L, 2L, "p2", 222L, 22L, "a2"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
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
        checkRowFields(paRowType);
        RowType pcRowType = plan.rowType();
        checkRowFields(pcRowType);
        Row[] expected = new Row[]{
            row(ancestorRowType, 3L, "a3"),
            row(parentRowType, 33L, 3L, "p3"),
            row(beforeChildRowType, 333L, 33L, "b3"),
            row(pcRowType, 33L, 3L, "p3", null, null, null),
            row(paRowType, 33L, 3L, "p3", null, null, null),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
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
        checkRowFields(paRowType);
        RowType pcRowType = plan.rowType();
        checkRowFields(pcRowType);
        Row[] expected = new Row[]{
            row(ancestorRowType, 4L, "a4"),
            row(parentRowType, 41L, 4L, "p41"),
            row(pcRowType, 41L, 4L, "p41", null, null, null),
            row(paRowType, 41L, 4L, "p41", null, null, null),
            row(parentRowType, 42L, 4L, "p42"),
            row(pcRowType, 42L, 4L, "p42", null, null, null),
            row(paRowType, 42L, 4L, "p42", null, null, null),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
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
        checkRowFields(pcRowType);
        Row[] expected = new Row[]{
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
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    // For use by this class

    private ExpressionGenerator selectAncestor(long aid)
    {
        return compare(field(ancestorRowType, 0), Comparison.EQ, literal(aid), castResolver());
    }

    // Object state

    private int ancestor;
    private int parent;
    private int beforeChild;
    private int child;
    private int afterChild;
    private TableRowType ancestorRowType;
    private TableRowType parentRowType;
    private TableRowType beforeChildRowType;
    private TableRowType childRowType;
    private TableRowType afterChildRowType;
    private IndexRowType parentPidIndexType;
    private Group group;
}
