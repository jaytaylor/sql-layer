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

package com.foundationdb.qp.operator;

import com.foundationdb.ais.model.HKeySegment;
import com.foundationdb.qp.row.FlattenedRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.FlattenedRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import static com.foundationdb.qp.operator.API.FlattenOption.KEEP_CHILD;
import static com.foundationdb.qp.operator.API.FlattenOption.KEEP_PARENT;

/**

 <h1>Overview</h1>

 Flatten_HKeyOrdered combines parent and child rows within a group to form a flattened row, containing all the fields of both rows. This implementation assumes that the rows being flattened are in hkey order. Rows whose type is other than the parent type or the child type are written to the output stream.

 Flatten is an intra-group join, so join semantics can be specified, (inner join, left outer, right outer).

 <h1>Arguments</h1>

 <ul>

 <li><b>RowType parentType:</b> Type of parent rows to be flattened.

 <li><b>RowType childType:</b> Type of child rows to be flattened.

 <li><b>JoinType:</b> Controls join semantics

 <li><b>EnumSet<FlattenOption>:</b> other flatten options

 </ul>

 JoinType:

 <ul>
 <li>INNER_JOIN
 <li>LEFT_JOIN
 <li>RIGHT_JOIN
 <li>FULL_JOIN
 </ul>

 FlattenOptions:

 <ul>
 <li>KEEP_PARENT: Copy parent rows to output.
 <li>KEEP_CHILD: Copy child rows to output.
 <li>LEFT_JOIN_SHORTENS_HKEY: If doing a left join, any generated row will have the hkey of its left row
 </ul>

 These options can be used individually or together.

 LEFT_JOIN_SHORTENS_HKEY is only valid if doing a LEFT_JOIN. With it,
 generated rows have the hkey of the left row, rather than of the right
 row with null values. For instance, if doing a C-O join, a customer
 with no orders will generate a C-O row with an HKey of
 (cid, nllu) by default, but (cid) with LEFT_JOIN_SHORTENS_HKEY enabled.

 parentType must be the immediate ancestor of childType. I.e., there
 must not be any other type, T, such that parentType is the ancestor of
 T, and T is the ancestor of childType.


 <h1>Behavior</h1>

 A row of parentType is recorded for later use, in processing child
 rows.

 A row of childType is flattened with the current parentType row and
 written to output. The parent and child rows are written to the output
 stream, in hkey order relative to the flattened row, under control of
 the KEEP_PARENT and KEEP_CHILD flags.

 Join semantics are implemented by tracking parents without children
 (for left join), children without parents (for right join), and
 parents with at least one child (for inner join). In the outer join
 cases, the flattened row contains nulls for fields from the missing
 row.

 Rows of other types are passed through from the input stream to the
 output stream.

 <h1>Output</h1>

 A flattened row contains all the fields of the parentType followed by
 all the fields of the childType. For left and right joins, the fields
 of the missing type are null.

 When a parentType row and childType row are flattened, the hkey of the
 flattened row is the same as that of the child, with nulls provided as
 necessary in outerjoin cases. There is one exception to this: for a
 left join row with null child LEFT_JOIN_SHORTENS_HKEY causes the
 flattened row's hkey to be that of the parent. (This is useful in
 processing group index updates, and is not expected to be used in an
 execution plan resulting from a SELECT statement.)

 <h1>Assumptions</h1>

 The input stream is hkey-ordered, at least with respect to rows of
 parentType and childType. For example, flattening Customer and Order
 requires that each Customer is followed by all of its Orders (before
 another Customer appears in the input), but does not require any
 ordering among Customers.

 h2. Performance

 Flatten_HKeyOrdered does no IO. For each input row, the type is
 determined, ancestry is checked, and for child rows, an output row is
 generated.

 h2. Memory Requirements

 Rows are combined by creating a wrapper of input parent and child
 rows, so while memory is consumed, there is no copying of fields from
 one row to another. The operator stores up to three input rows at a
 time.

 */

class Flatten_HKeyOrdered extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder(getClass().getSimpleName());
        buffer.append("(");
        buffer.append(parentType);
        if (keepParent) {
            buffer.append(" KEEP");
        }
        if (leftJoin && rightJoin) {
            buffer.append(" FULL ");
        } else if (leftJoin) {
            buffer.append(" LEFT ");
        } else if (rightJoin) {
            buffer.append(" RIGHT ");
        } else {
            buffer.append(" INNER ");
        }
        buffer.append(childType);
        if (keepChild) {
            buffer.append(" KEEP");
        }
        buffer.append(")");
        return buffer.toString();
    }

    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, inputOperator.cursor(context, bindingsCursor));
    }

    @Override
    public FlattenedRowType rowType()
    {
        return flattenType;
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
        derivedTypes.add(flattenType);
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<>(1);
        result.add(inputOperator);
        return result;
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Flatten_HKeyOrdered interface

    public Flatten_HKeyOrdered(Operator inputOperator,
                               RowType parentType,
                               RowType childType,
                               API.JoinType joinType,
                               Set<API.FlattenOption> options)
    {
        ArgumentValidation.notNull("parentType", parentType);
        ArgumentValidation.notNull("childType", childType);
        ArgumentValidation.notNull("flattenType", joinType);
        ArgumentValidation.isTrue("parentType really is parent of childType", parentType.parentOf(childType));
        this.inputOperator = inputOperator;
        this.parentType = parentType;
        this.childType = childType;
        this.flattenType = parentType.schema().newFlattenType(parentType, childType);
        boolean fullJoin = joinType.equals(API.JoinType.FULL_JOIN);
        this.leftJoin = fullJoin || joinType.equals(API.JoinType.LEFT_JOIN);
        this.rightJoin = fullJoin || joinType.equals(API.JoinType.RIGHT_JOIN);
        this.keepParent = options.contains(KEEP_PARENT);
        this.keepChild = options.contains(KEEP_CHILD);
        
        List<HKeySegment> childHKeySegments = childType.hKey().segments();
        HKeySegment lastChildHKeySegment = childHKeySegments.get(childHKeySegments.size() - 1);
        RowDef childRowDef = lastChildHKeySegment.table().rowDef();
        this.childOrdinal = childRowDef.table().getOrdinal();
        //this.nChildHKeySegmentFields = lastChildHKeySegment.columns().size();
        //this.parentHKeySegments = parentType.hKey().segments().size();
    }

    // Class state

    private static final int MAX_PENDING = 2;
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Flatten_HKeyOrdered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Flatten_HKeyOrdered next");
    private static final Logger LOG = LoggerFactory.getLogger(Flatten_HKeyOrdered.class);

    // Object state

    private final Operator inputOperator;
    private final RowType parentType;
    private final RowType childType;
    private final FlattenedRowType flattenType;
    private final boolean leftJoin;
    private final boolean rightJoin;
    private final boolean keepParent;
    private final boolean keepChild;
    // For constructing a left-join hkey
    private final int childOrdinal;
    //private final int nChildHKeySegmentFields;
    //private final int parentHKeySegments;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
       
        Attributes atts = new Attributes();
        
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        if (keepParent) 
            atts.put(Label.FLATTEN_OPTION, PrimitiveExplainer.getInstance("KEEP PARENT"));
        if (keepChild) 
            atts.put(Label.FLATTEN_OPTION, PrimitiveExplainer.getInstance("KEEP CHILD"));
        if (leftJoin)
            if (rightJoin)
                atts.put(Label.JOIN_OPTION, PrimitiveExplainer.getInstance("FULL"));
            else
                atts.put(Label.JOIN_OPTION, PrimitiveExplainer.getInstance("LEFT"));
        else
            if (rightJoin)
                atts.put(Label.JOIN_OPTION, PrimitiveExplainer.getInstance("RIGHT"));
            else
                atts.put(Label.JOIN_OPTION, PrimitiveExplainer.getInstance("INNER"));
        atts.put(Label.PARENT_TYPE, parentType.getExplainer(context));
        atts.put(Label.CHILD_TYPE, childType.getExplainer(context));
        atts.put(Label.INPUT_OPERATOR, inputOperator.getExplainer(context));
        
        return new CompoundExplainer(Type.FLATTEN_OPERATOR, atts);
    }

    // Inner classes

    private class Execution extends ChainedCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                super.open();
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                if (CURSOR_LIFECYCLE_ENABLED) {
                    CursorLifecycle.checkIdleOrActive(this);
                }
                checkQueryCancelation();
                Row outputRow = pending.poll();
                Row inputRow;
                while (outputRow == null && (((inputRow = input.next()) != null) || (parent != null))) {
                    if (readyForLeftJoinRow(inputRow)) {
                        // child rows are processed immediately. parent rows are not,
                        // because when seen, we don't know if the next row will be another
                        // parent, a child, a row of child type that is not actually a child,
                        // a row of type other than parent or child, or end of stream. If inputRow is
                        // null, then input is exhausted, and the only possibly remaining row would
                        // be due to a childless parent waiting to be processed.
                        generateLeftJoinRow(parent);
                    }
                    if (inputRow == null) {
                        // With inputRow == null, we needed parent to create a left join row if required. That's
                        // been done. set parent to null so that the loop exits.
                        setParent(null);
                    } else {
                        RowType inputRowType = inputRow.rowType();
                        if (inputRowType == parentType) {
                            if (keepParent) {
                                addToPending(inputRow);
                            }
                            setParent(inputRow);
                        } else if (inputRowType == childType) {
                            if (keepChild) {
                                addToPending(inputRow);
                            }
                            if (parent != null && parent.ancestorOf(inputRow)) {
                                // child is not an orphan
                                generateInnerJoinRow(parent, inputRow);
                                childlessParent = false;
                            } else {
                                // child is an orphan
                                setParent(null);
                                generateRightJoinRow(inputRow);
                            }
                        } else {
                            addToPending(inputRow);
                        }
                    }
                    outputRow = pending.poll();
                }
                if (outputRow == null) { setIdle(); }
                if (LOG_EXECUTION) {
                    LOG.debug("Flatten_HKeyOrdered: yield {}", outputRow);
                }
                return outputRow;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context, input);
            this.leftJoinHKey = adapter().newHKey(childType.hKey());
            //LOG.error("Parent: {}, Child: {}", parentType, childType);
        }

        // For use by this class

        private void generateInnerJoinRow(Row parent, Row child)
        {
            assert parent != null;
            assert child != null;
            pending.add(new FlattenedRow(flattenType, parent, child, child.hKey()));
        }

        private void generateLeftJoinRow(Row parent)
        {
            assert parent != null;
            if (leftJoin) {
                assert parent.hKey().segments() == parentType.hKey().segments().size() : 
                    String.format("%s: parent hkey %s has been shortened by an earlier Flatten, " +
                        "so this Flatten should specify LEFT_JOIN_SHORTENS_HKEY also",
                        this, parent.hKey());
                // Copy leftJoinHKey to avoid aliasing problems. (leftJoinHKey changes on each parent row.)
                HKey hKey = adapter().newHKey(childType.hKey());
                leftJoinHKey.copyTo(hKey);
                pending.add(new FlattenedRow(flattenType, parent, null, hKey));
                // Prevent generation of another left join row for the same parent
                childlessParent = false;
            }
        }

        private void generateRightJoinRow(Row child)
        {
            assert child != null;
            if (rightJoin) {
                pending.add(new FlattenedRow(flattenType, null, child, child.hKey()));
            }
        }

        private void addToPending(Row row)
        {
            pending.add(row);
        }

        private void setParent(Row newParent)
        {
            parent = newParent;
            if (leftJoin && newParent != null) {
                computeLeftJoinHKey(newParent);
                childlessParent = true;
            }
        }

        private void computeLeftJoinHKey(Row newParent)
        {
            HKey parentHKey = newParent.hKey();
            parentHKey.copyTo(leftJoinHKey);
            leftJoinHKey.extendWithOrdinal(childOrdinal);
            leftJoinHKey.extendWithNull();
            
            //for (int i = 0; i < nChildHKeySegmentFields; i++) {
            //    leftJoinHKey.extendWithNull();
            //}
        }

        private boolean readyForLeftJoinRow(Row inputRow)
        {
            boolean readyForLeftJoinRow = false;
            if (leftJoin && childlessParent && parent != null) {
                if (inputRow == null) {
                    readyForLeftJoinRow = true;
                } else if (inputRow.rowType() == parentType) {
                    readyForLeftJoinRow = true;
                } else if (!parent.ancestorOf(inputRow)) {
                    readyForLeftJoinRow = true;
                } else if (inputRow.rowType() == childType) {
                    // inputRow is a child of parent (since ancestorOf is true)
                    readyForLeftJoinRow = false;
                } else {
                    // Equality can occur when an input stream contains both flattened rows and the input
                    // to the flattening, e.g. an Order row, o, and a flatten(Customer, Order)
                    // row derived from o. Ordering is not well-defined in this case, so neither < nor <= is
                    // clearly preferable.
                    readyForLeftJoinRow = leftJoinHKey.compareTo(inputRow.hKey()) < 0;
                }
            }
            return readyForLeftJoinRow;
        }

        // Object state

        private Row parent;
        private final Queue<Row> pending = new ArrayDeque<>(MAX_PENDING);
        private final HKey leftJoinHKey;
        private boolean childlessParent;
    }
}
