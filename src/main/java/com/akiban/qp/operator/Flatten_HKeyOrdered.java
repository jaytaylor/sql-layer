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

package com.akiban.qp.operator;

import com.akiban.ais.model.HKeySegment;
import com.akiban.qp.exec.Plannable;
import com.akiban.qp.row.FlattenedRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.FlattenedRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.rowdata.RowDef;
import com.akiban.sql.optimizer.explain.*;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.akiban.qp.operator.API.FlattenOption.KEEP_CHILD;
import static com.akiban.qp.operator.API.FlattenOption.KEEP_PARENT;
import java.util.Map;

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
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context, inputOperator.cursor(context));
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
        List<Operator> result = new ArrayList<Operator>(1);
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
        this.childOrdinal = childRowDef.getOrdinal();
        this.nChildHKeySegmentFields = lastChildHKeySegment.columns().size();
        this.parentHKeySegments = parentType.hKey().segments().size();
    }

    // Class state

    private static final int MAX_PENDING = 2;
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Flatten_HKeyOrdered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Flatten_HKeyOrdered next");

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
    private final int nChildHKeySegmentFields;
    private final int parentHKeySegments;

    @Override
    public Explainer getExplainer(Map<Object, Explainer> extraInfo)
    {
       
        Attributes atts = new Attributes();
        
        atts.put(Label.NAME, PrimitiveExplainer.getInstance("Flatten_HKeyOrdered"));
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
        atts.put(Label.PARENT_TYPE, parentType.getExplainer(extraInfo));
        atts.put(Label.CHILD_TYPE, childType.getExplainer(extraInfo));
        atts.put(Label.INPUT_OPERATOR, inputOperator.getExplainer(extraInfo));
        
        return new OperationExplainer(Type.FLATTEN_OPERATOR, atts);
    }

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                input.open();
                idle = false;
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            TAP_NEXT.in();
            try {
                CursorLifecycle.checkIdleOrActive(this);
                checkQueryCancelation();
                Row outputRow = pending.take();
                Row inputRow;
                while (outputRow == null && (((inputRow = input.next()) != null) || parent.isHolding())) {
                    if (readyForLeftJoinRow(inputRow)) {
                        // child rows are processed immediately. parent rows are not,
                        // because when seen, we don't know if the next row will be another
                        // parent, a child, a row of child type that is not actually a child,
                        // a row of type other than parent or child, or end of stream. If inputRow is
                        // null, then input is exhausted, and the only possibly remaining row would
                        // be due to a childless parent waiting to be processed.
                        generateLeftJoinRow(parent.get());
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
                            if (parent.isHolding() && parent.get().ancestorOf(inputRow)) {
                                // child is not an orphan
                                generateInnerJoinRow(parent.get(), inputRow);
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
                    outputRow = pending.take();
                }
                idle = outputRow == null;
                return outputRow;
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close()
        {
            parent.release();
            pending.clear();
            input.close();
            idle = true;
        }

        @Override
        public void destroy()
        {
            close();
            input.destroy();
        }

        @Override
        public boolean isIdle()
        {
            return idle;
        }

        @Override
        public boolean isActive()
        {
            return !idle;
        }

        @Override
        public boolean isDestroyed()
        {
            return input.isDestroyed();
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context);
            this.input = input;
            this.leftJoinHKey = adapter().newHKey(childType.hKey());
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
                if (parent.hKey().segments() < parentHKeySegments) {
                    throw new IncompatibleRowException(
                        String.format("%s: parent hkey %s has been shortened by an earlier Flatten, " +
                                      "so this Flatten should specify LEFT_JOIN_SHORTENS_HKEY also",
                                      this, parent.hKey()));
                }
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
            parent.hold(newParent);
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
            for (int i = 0; i < nChildHKeySegmentFields; i++) {
                leftJoinHKey.extendWithNull();
            }
        }

        private boolean readyForLeftJoinRow(Row inputRow)
        {
            boolean readyForLeftJoinRow = false;
            if (leftJoin && childlessParent && parent.isHolding()) {
                if (inputRow == null) {
                    readyForLeftJoinRow = true;
                } else if (inputRow.rowType() == parentType) {
                    readyForLeftJoinRow = true;
                } else if (!parent.get().ancestorOf(inputRow)) {
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

        private final Cursor input;
        private final ShareHolder<Row> parent = new ShareHolder<Row>();
        private final PendingRows pending = new PendingRows(MAX_PENDING);
        private final HKey leftJoinHKey;
        private boolean childlessParent;
        private boolean idle = true;
    }
}
