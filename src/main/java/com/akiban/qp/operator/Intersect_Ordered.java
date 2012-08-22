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

import com.akiban.qp.exec.Plannable;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.IndexRowPrefixSelector;
import com.akiban.server.explain.*;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.akiban.qp.operator.API.IntersectOption;
import static com.akiban.qp.operator.API.JoinType;
import static java.lang.Math.abs;
import static java.lang.Math.min;

/**
 * <h1>Overview</h1>
 * <p/>
 * Intersect_Ordered finds rows from one of two input streams whose projection onto a set of common fields matches
 * a row in the other stream. Each input stream must be ordered by at least these common fields.
 * For each matching pair of rows, output from the selected input stream is emitted as output.
 * <p/>
 * <h1>Arguments</h1>
 * <p/>
 * <li><b>Operator left:</b> Operator providing left input stream.
 * <li><b>Operator right:</b> Operator providing right input stream.
 * <li><b>IndexRowType leftRowType:</b> Type of rows from left input stream.
 * <li><b>IndexRowType rightRowType:</b> Type of rows from right input stream.
 * <li><b>int leftOrderingFields:</b> Number of trailing fields of left input rows to be used for ordering and matching rows.
 * <li><b>int rightOrderingFields:</b> Number of trailing fields of right input rows to be used for ordering and matching rows.
 * <li><b>boolean[] ascending:</b> The length of this array specifies the number of fields to be compared in the merge
 * for the purpose of determining whether a left row and right row agree and will result in an output row,
 * ascending.length <= min(leftOrderingFields, rightOrderingFields). ascending[i] is true if the ith such field
 * is ascending, false if it is descending. This ordering specification must be consistent with the order of both
 * input streams.
 * <li><b>JoinType joinType:</b>
 * <ul>
 * <li>INNER_JOIN: An ordinary intersection is computed.
 * <li>LEFT_JOIN: Keep an unmatched row from the left input stream, filling out the row with nulls
 * <li>RIGHT_JOIN: Keep an unmatched row from the right input stream, filling out the row with nulls
 * <li>FULL_JOIN: Not supported
 * </ul>
 * (Nothing else is supported currently).
 * <li><b>IntersectOption intersectOutput:</b> OUTPUT_LEFT or OUTPUT_RIGHT, depending on which streams rows
 * should be emitted as output.
 * <p/>
 * <h1>Behavior</h1>
 * <p/>
 * The two streams are merged, looking for pairs of rows, one from each input stream, which match in the common
 * fields. When such a match is found, a row from the stream selected by <tt>intersectOutput</tt> is emitted.
 * <p/>
 * <h1>Output</h1>
 * <p/>
 * Rows that match at least one row in the other input stream.
 * <p/>
 * <h1>Assumptions</h1>
 * <p/>
 * Each input stream is ordered by its ordering columns, as determined by <tt>leftOrderingFields</tt>
 * and <tt>rightOrderingFields</tt>, and ordered according to <tt>ascending</tt>. The order of rows in both
 * input streams must be consistent with <tt>ascending.</tt>
 * <p/>
 * The input row types must correspond to indexes in the same group (as determined by the index's leafmost table).
 * This constraint may be relaxed in the future, but then the number of fields to compare is likely to be required
 * as a new constructor argument.
 * <p/>
 * <h1>Performance</h1>
 * <p/>
 * This operator does no IO.
 * <p/>
 * <h1>Memory Requirements</h1>
 * <p/>
 * Two input rows, one from each stream.
 */

class Intersect_Ordered extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(skip %d from left, skip %d from right, compare %d%s)",
                             getClass().getSimpleName(), leftFixedFields, rightFixedFields, fieldsToCompare, skipScan ? ", SKIP_SCAN" : "");
    }

    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context);
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        right.findDerivedTypes(derivedTypes);
        left.findDerivedTypes(derivedTypes);
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<Operator>(2);
        result.add(left);
        result.add(right);
        return result;
    }

    @Override
    public String describePlan()
    {
        return String.format("%s\n%s", describePlan(left), describePlan(right));
    }

    // Intersect_Ordered interface

    public Intersect_Ordered(Operator left,
                             Operator right,
                             IndexRowType leftRowType,
                             IndexRowType rightRowType,
                             int leftOrderingFields,
                             int rightOrderingFields,
                             boolean[] ascending,
                             JoinType joinType,
                             EnumSet<IntersectOption> options,
                             boolean usePValues)
    {
        ArgumentValidation.notNull("left", left);
        ArgumentValidation.notNull("right", right);
        ArgumentValidation.notNull("leftRowType", leftRowType);
        ArgumentValidation.notNull("rightRowType", rightRowType);
        ArgumentValidation.notNull("joinType", joinType);
        ArgumentValidation.isGTE("leftOrderingFields", leftOrderingFields, 0);
        ArgumentValidation.isLTE("leftOrderingFields", leftOrderingFields, leftRowType.nFields());
        ArgumentValidation.isGTE("rightOrderingFields", rightOrderingFields, 0);
        ArgumentValidation.isLTE("rightOrderingFields", rightOrderingFields, rightRowType.nFields());
        ArgumentValidation.isGTE("ascending.length()", ascending.length, 0);
        ArgumentValidation.isLTE("ascending.length()", ascending.length, min(leftOrderingFields, rightOrderingFields));
        ArgumentValidation.isNotSame("joinType", joinType, "JoinType.FULL_JOIN", JoinType.FULL_JOIN);
        ArgumentValidation.notNull("options", options);
        ArgumentValidation.isSame("leftRowType group", leftRowType.index().leafMostTable().getGroup(),
                                  "rightRowType group", rightRowType.index().leafMostTable().getGroup());
        // scan algorithm
        boolean skipScan = options.contains(IntersectOption.SKIP_SCAN);
        boolean sequentialScan = options.contains(IntersectOption.SEQUENTIAL_SCAN);
        // skip scan is the default until everyone is explicit about it
        if (!skipScan && !sequentialScan) {
            skipScan = true;
        }
        ArgumentValidation.isTrue("options for scanning",
                                  (skipScan || sequentialScan) &&
                                  !(skipScan && sequentialScan));
        this.skipScan = skipScan;
        // output
        this.outputLeft = options.contains(IntersectOption.OUTPUT_LEFT);
        boolean outputRight = options.contains(IntersectOption.OUTPUT_RIGHT);
        ArgumentValidation.isTrue("options for output",
                                  (outputLeft || outputRight) &&
                                  !(outputLeft && outputRight));
        ArgumentValidation.isTrue("joinType consistent with intersectOutput",
                                  joinType == JoinType.INNER_JOIN ||
                                  joinType == JoinType.LEFT_JOIN && options.contains(IntersectOption.OUTPUT_LEFT) ||
                                  joinType == JoinType.RIGHT_JOIN && options.contains(IntersectOption.OUTPUT_RIGHT));
        this.left = left;
        this.right = right;
        this.leftRowType = leftRowType;
        this.rightRowType = rightRowType;
        this.joinType = joinType;
        this.ascending = Arrays.copyOf(ascending, ascending.length);
        // outerjoins
        this.keepUnmatchedLeft = joinType == JoinType.LEFT_JOIN;
        this.keepUnmatchedRight = joinType == JoinType.RIGHT_JOIN;
        // Setup for row comparisons
        this.leftFixedFields = leftRowType.nFields() - leftOrderingFields;
        this.rightFixedFields = rightRowType.nFields() - rightOrderingFields;
        this.fieldsToCompare = ascending.length;
        // Setup for jumping
        leftSkipRowColumnSelector = new IndexRowPrefixSelector(leftFixedFields + fieldsToCompare);
        rightSkipRowColumnSelector = new IndexRowPrefixSelector(rightFixedFields + fieldsToCompare);
        this.usePValues = usePValues;
    }

    // For use by this class

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Intersect_Ordered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Intersect_Ordered next");
    private static final Logger LOG = LoggerFactory.getLogger(Intersect_Ordered.class);

    // Object state

    private final Operator left;
    private final Operator right;
    private final IndexRowType leftRowType;
    private final IndexRowType rightRowType;
    private final JoinType joinType;
    private final int leftFixedFields;
    private final int rightFixedFields;
    private final int fieldsToCompare;
    private final boolean keepUnmatchedLeft;
    private final boolean keepUnmatchedRight;
    private final boolean outputLeft;
    private final boolean skipScan;
    private final boolean[] ascending;
    private final ColumnSelector leftSkipRowColumnSelector;
    private final ColumnSelector rightSkipRowColumnSelector;
    private final boolean usePValues;

    @Override
    public Explainer getExplainer(Map<Object, Explainer> extraInfo)
    {
        Attributes atts = new Attributes();
        
        atts.put(Label.NAME, PrimitiveExplainer.getInstance("Intersect_Ordered"));
        atts.put(Label.LEFT, PrimitiveExplainer.getInstance(leftFixedFields));
        atts.put(Label.RIGHT, PrimitiveExplainer.getInstance(rightFixedFields));
        atts.put(Label.NUM_COMPARE, PrimitiveExplainer.getInstance(fieldsToCompare));
        atts.put(Label.JOIN_OPTION, PrimitiveExplainer.getInstance(joinType.name()));
        atts.put(Label.INPUT_OPERATOR, left.getExplainer(extraInfo));
        atts.put(Label.INPUT_OPERATOR, right.getExplainer(extraInfo));
        return new CompoundExplainer(Type.ORDERED, atts);
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
                leftInput.open();
                rightInput.open();
                nextLeftRow();
                nextRightRow();
                closed = leftRow.isEmpty() && rightRow.isEmpty();
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
                Row next = null;
                while (!closed && next == null) {
                    assert !(leftRow.isEmpty() && rightRow.isEmpty());
                    long c = compareRows();
                    if (c < 0) {
                        if (keepUnmatchedLeft) {
                            assert outputLeft;
                            next = leftRow.get();
                            nextLeftRow();
                        } else {
                            if (skipScan) {
                                nextLeftRowSkip(rightRow.get(), rightFixedFields, leftSkipRowColumnSelector);
                            } else {
                                nextLeftRow();
                            }
                        }
                    } else if (c > 0) {
                        if (keepUnmatchedRight) {
                            assert !outputLeft;
                            next = rightRow.get();
                            nextRightRow();
                        } else {
                            if (skipScan) {
                                nextRightRowSkip(leftRow.get(), leftFixedFields, rightSkipRowColumnSelector);
                            } else {
                                nextRightRow();
                            }
                        }
                    } else {
                        // left and right rows match
                        if (outputLeft) {
                            next = leftRow.get();
                            nextLeftRow();
                        } else {
                            next = rightRow.get();
                            nextRightRow();
                        }
                    }
                    boolean leftEmpty = leftRow.isEmpty();
                    boolean rightEmpty = rightRow.isEmpty();
                    if (leftEmpty && rightEmpty ||
                        leftEmpty && !keepUnmatchedRight ||
                        rightEmpty && !keepUnmatchedLeft) {
                        close();
                    }
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Intersect_Ordered: yield {}", next);
                }
                return next;
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void jump(Row jumpRow, ColumnSelector jumpRowColumnSelector)
        {
            // This operator emits rows from left or right. The row used to specify the jump should be of the matching
            // row type.
            int suffixRowFixedFields;
            if (outputLeft) {
                assert jumpRow.rowType() == leftRowType : jumpRow.rowType();
                suffixRowFixedFields = leftFixedFields;
            } else {
                assert jumpRow.rowType() == rightRowType : jumpRow.rowType();
                suffixRowFixedFields = rightFixedFields;
            }
            nextLeftRowSkip(jumpRow, suffixRowFixedFields, jumpRowColumnSelector);
            nextRightRowSkip(jumpRow, suffixRowFixedFields, jumpRowColumnSelector);
            if (leftRow.isEmpty() || rightRow.isEmpty()) {
                close();
            }
        }

        @Override
        public void close()
        {
            CursorLifecycle.checkIdleOrActive(this);
            if (!closed) {
                leftRow.release();
                rightRow.release();
                leftInput.close();
                rightInput.close();
                closed = true;
            }
        }

        @Override
        public void destroy()
        {
            close();
            leftInput.destroy();
            rightInput.destroy();
        }

        @Override
        public boolean isIdle()
        {
            return closed;
        }

        @Override
        public boolean isActive()
        {
            return !closed;
        }

        @Override
        public boolean isDestroyed()
        {
            assert leftInput.isDestroyed() == rightInput.isDestroyed();
            return leftInput.isDestroyed();
        }

        // Execution interface

        Execution(QueryContext context)
        {
            super(context);
            leftInput = left.cursor(context);
            rightInput = right.cursor(context);
        }

        // For use by this class

        private void nextLeftRow()
        {
            Row row = leftInput.next();
            leftRow.hold(row);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Intersect_Ordered: left {}", row);
            }
        }

        private void nextRightRow()
        {
            Row row = rightInput.next();
            rightRow.hold(row);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Intersect_Ordered: right {}", row);
            }
        }

        private long compareRows()
        {
            long c;
            assert !closed;
            assert !(leftRow.isEmpty() && rightRow.isEmpty());
            if (leftRow.isEmpty()) {
                c = 1;
            } else if (rightRow.isEmpty()) {
                c = -1;
            } else {
                c = leftRow.get().compareTo(rightRow.get(), leftFixedFields, rightFixedFields, fieldsToCompare);
                if (c != 0) {
                    int fieldThatDiffers = (int) abs(c) - 1;
                    assert fieldThatDiffers < ascending.length;
                    if (!ascending[fieldThatDiffers]) {
                        c = -c;
                    }
                }
            }
            return c;
        }

        private void nextLeftRowSkip(Row jumpRow, int jumpRowFixedFields, ColumnSelector jumpRowColumnSelector)
        {
            if (leftRow.isHolding()) {
                addSuffixToSkipRow(leftSkipRow(),
                                   leftFixedFields,
                                   jumpRow,
                                   jumpRowFixedFields);
                leftInput.jump(leftSkipRow, jumpRowColumnSelector);
                leftRow.hold(leftInput.next());
            }
        }

        private void nextRightRowSkip(Row jumpRow, int jumpRowFixedFields, ColumnSelector jumpRowColumnSelector)
        {
            if (rightRow.isHolding()) {
                addSuffixToSkipRow(rightSkipRow(),
                                   rightFixedFields,
                                   jumpRow,
                                   jumpRowFixedFields);
                rightInput.jump(rightSkipRow, jumpRowColumnSelector);
                rightRow.hold(rightInput.next());
            }
        }

        private void addSuffixToSkipRow(ValuesHolderRow skipRow,
                                        int skipRowFixedFields,
                                        Row jumpRow,
                                        int jumpRowFixedFields)
        {
            boolean usingPValues = skipRow.usingPValues();
            assert Intersect_Ordered.this.usePValues == usingPValues : "unexpected skiprow: " + usingPValues;
            if (jumpRow == null) {
                for (int f = 0; f < fieldsToCompare; f++) {
                    if (usingPValues)
                        skipRow.pvalueAt(skipRowFixedFields + f).putNull();
                    else
                        skipRow.holderAt(skipRowFixedFields + f).putNull();
                }
            } else {
                for (int f = 0; f < fieldsToCompare; f++) {
                    if (usingPValues)
                        PValueTargets.copyFrom(
                                jumpRow.pvalue(jumpRowFixedFields + f),
                                skipRow.pvalueAt(skipRowFixedFields + f));
                    else
                        skipRow.holderAt(skipRowFixedFields + f).copyFrom(jumpRow.eval(jumpRowFixedFields + f));
                }
            }
        }

        private ValuesHolderRow leftSkipRow()
        {
            if (leftSkipRow == null) {
                assert leftRow.isHolding();
                boolean usingPValues = Intersect_Ordered.this.usePValues;
                leftSkipRow = new ValuesHolderRow(leftRowType, usingPValues);
                int f = 0;
                while (f < leftFixedFields) {
                    if (usingPValues)
                        PValueTargets.copyFrom(
                                leftRow.get().pvalue(f),
                                leftSkipRow.pvalueAt(f));
                    else
                        leftSkipRow.holderAt(f).copyFrom(leftRow.get().eval(f));
                    f++;
                }
                while (f < leftRowType.nFields()) {
                    if (usingPValues)
                        leftSkipRow.pvalueAt(f++).putNull();
                    else
                        leftSkipRow.holderAt(f++).putNull();
                }
            }
            return leftSkipRow;
        }

        private ValuesHolderRow rightSkipRow()
        {
            if (rightSkipRow == null) {
                assert rightRow.isHolding();
                boolean usingPValues = Intersect_Ordered.this.usePValues;
                rightSkipRow = new ValuesHolderRow(rightRowType, usingPValues);
                int f = 0;
                while (f < rightFixedFields) {
                    if (usingPValues)
                        PValueTargets.copyFrom(
                                rightRow.get().pvalue(f),
                                rightSkipRow.pvalueAt(f));
                    else
                        rightSkipRow.holderAt(f).copyFrom(rightRow.get().eval(f));
                    f++;
                }
                while (f < rightRowType.nFields()) {
                    if (usingPValues)
                        rightSkipRow.pvalueAt(f++).putNull();
                    else
                        rightSkipRow.holderAt(f++).putNull();
                }
            }
            return rightSkipRow;
        }

        // Object state

        // Rows from each input stream are bound to the QueryContext. However, QueryContext doesn't use
        // ShareHolders, so they are needed here.

        private boolean closed = true;
        private final Cursor leftInput;
        private final Cursor rightInput;
        private final ShareHolder<Row> leftRow = new ShareHolder<Row>();
        private final ShareHolder<Row> rightRow = new ShareHolder<Row>();
        private ValuesHolderRow leftSkipRow;
        private ValuesHolderRow rightSkipRow;
    }
}
