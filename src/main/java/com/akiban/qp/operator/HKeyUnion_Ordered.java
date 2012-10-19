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

import com.akiban.qp.row.HKey;
import com.akiban.qp.row.HKeyRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.HKeyRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.qp.util.HKeyCache;
import com.akiban.server.expression.std.AbstractTwoArgExpressionEvaluation;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.expression.std.RankExpression;
import com.akiban.server.explain.*;
import com.akiban.server.types3.Types3Switch;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.lang.Math.min;

/**
 <h1>Overview</h1>

 HKeyUnion_Ordered returns the union of hkeys from its two input streams. The hkeys are shortened to a
 specified ancestor. Duplicate hkeys are eliminated.

 <h1>Arguments</h1>

<li><b>Operator left:</b> Operator providing left input stream.
<li><b>Operator right:</b> Operator providing right input stream.
<li><b>IndexRowType leftRowType:</b> Type of rows from left input stream.
<li><b>IndexRowType rightRowType:</b> Type of rows from right input stream.
<li><b>int leftOrderingFields:</b> Number of trailing fields of left input rows to be used for ordering and matching rows.
<li><b>int rightOrderingFields:</b> Number of trailing fields of right input rows to be used for ordering and matching rows.
<li><b>int comparisonFields:</b> Number of ordering fields to be compared.
<li><b>UserTableRowType outputHKeyTableRowType:</b> Before eliminating duplicate hkeys, hkeys from the input stream
 are shortened to match <tt>outputHKeyTableRowType</tt>'s table's hkey type.

 <h1>Behavior</h1>

 The input streams are in hkey order. The streams are merged, hkeys are shortened, and output rows containing the
 hkey are emitted.

 <h1>Output</h1>

 Shortened hkeys, each derived from one of the two input streams. Output is hkey-ordered.

 <h1>Assumptions</h1>

 Each input stream is hkey-ordered and has unique hkeys.

 <h1>Performance</h1>

 This operator does no IO.

 <h1>Memory Requirements</h1>

 Two input rows, one from each stream.

 */

class HKeyUnion_Ordered extends Operator
{
    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context);
    }

    @Override
    public RowType rowType()
    {
        return outputHKeyRowType;
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

    // HKeyUnion_Ordered interface

    public HKeyUnion_Ordered(Operator left,
                             Operator right,
                             RowType leftRowType,
                             RowType rightRowType,
                             int leftOrderingFields,
                             int rightOrderingFields,
                             int comparisonFields,
                             UserTableRowType outputHKeyTableRowType)
    {
        ArgumentValidation.notNull("left", left);
        ArgumentValidation.notNull("right", right);
        ArgumentValidation.notNull("leftRowType", leftRowType);
        ArgumentValidation.notNull("rightRowType", rightRowType);
        ArgumentValidation.isGTE("leftOrderingFields", leftOrderingFields, 0);
        ArgumentValidation.isLTE("leftOrderingFields", leftOrderingFields, leftRowType.nFields());
        ArgumentValidation.isGTE("rightOrderingFields", rightOrderingFields, 0);
        ArgumentValidation.isLTE("rightOrderingFields", rightOrderingFields, rightRowType.nFields());
        ArgumentValidation.isGTE("comparisonFields", comparisonFields, 0);
        ArgumentValidation.isLTE("comparisonFields", comparisonFields, min(leftOrderingFields, rightOrderingFields));
        ArgumentValidation.notNull("outputHKeyTableRowType", outputHKeyTableRowType);
        this.left = left;
        this.right = right;
        this.leftFields = leftOrderingFields;
        this.rightFields = rightOrderingFields;
        this.compareFields = comparisonFields;
        this.advanceLeftOnMatch = leftOrderingFields >= rightOrderingFields;
        this.advanceRightOnMatch = rightOrderingFields >= leftOrderingFields;
        this.outputHKeyTableRowType = outputHKeyTableRowType;
        com.akiban.ais.model.HKey outputHKeyDefinition = outputHKeyTableRowType.userTable().hKey();
        this.outputHKeyRowType = outputHKeyTableRowType.schema().newHKeyRowType(outputHKeyDefinition);
        this.outputHKeySegments = outputHKeyDefinition.segments().size();
        // Setup for row comparisons
        fieldRankingExpressionsAdapters = Types3Switch.ON
                ? null
                : new OldRankExpressions(leftRowType, rightRowType, leftOrderingFields, rightOrderingFields, comparisonFields);
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: HKeyUnion_Ordered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: HKeyUnion_Ordered next");

    // Object state

    private final Operator left;
    private final Operator right;
    private final RankExpressions fieldRankingExpressionsAdapters;
    private final boolean advanceLeftOnMatch;
    private final boolean advanceRightOnMatch;
    private final UserTableRowType outputHKeyTableRowType;
    private final HKeyRowType outputHKeyRowType;
    private final int outputHKeySegments;
    private final int leftFields;
    private final int rightFields;
    private final int compareFields;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.LEFT, PrimitiveExplainer.getInstance(leftFields));
        atts.put(Label.RIGHT, PrimitiveExplainer.getInstance(rightFields));
        atts.put(Label.NUM_COMPARE, PrimitiveExplainer.getInstance(compareFields));
        atts.put(Label.OUTPUT_TYPE, outputHKeyTableRowType.getExplainer(context));
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
                previousHKey = null;
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
                Row nextRow = null;
                while (!closed && nextRow == null) {
                    assert !(leftRow.isEmpty() && rightRow.isEmpty());
                    long c = compareRows();
                    if (c < 0) {
                        nextRow = leftRow.get();
                        nextLeftRow();
                    } else if (c > 0) {
                        nextRow = rightRow.get();
                        nextRightRow();
                    } else {
                        nextRow = leftRow.get();
                        if (advanceLeftOnMatch) {
                            nextLeftRow();
                        }
                        if (advanceRightOnMatch) {
                            nextRightRow();
                        }
                    }
                    if (leftRow.isEmpty() && rightRow.isEmpty()) {
                        close();
                    }
                    if (nextRow == null) {
                        close();
                    } else if (previousHKey == null || !previousHKey.prefixOf(nextRow.hKey())) {
                        HKey nextHKey = outputHKey(nextRow);
                        nextRow = new HKeyRow(outputHKeyRowType, nextHKey, hKeyCache);
                        previousHKey = nextHKey;
                    } else {
                        nextRow = null;
                    }
                }
                return nextRow;
            } finally {
                TAP_NEXT.out();
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
            hKeyCache = new HKeyCache<HKey>(context.getStore());
            rankEvaluations = fieldRankingExpressionsAdapters.buildEvaluations();
        }
        
        // For use by this class
        
        private void nextLeftRow()
        {
            Row row;
            do {
                row = leftInput.next();
            } while (row != null && previousHKey != null && previousHKey.prefixOf(row.hKey()));
            leftRow.hold(row);
        }
        
        private void nextRightRow()
        {
            Row row;
            do {
                row = rightInput.next();
            } while (row != null && previousHKey != null && previousHKey.prefixOf(row.hKey()));
            rightRow.hold(row);
        }
        
        private long compareRows()
        {
            long c = 0;
            assert !closed;
            assert !(leftRow.isEmpty() && rightRow.isEmpty());
            if (leftRow.isEmpty()) {
                c = 1;
            } else if (rightRow.isEmpty()) {
                c = -1;
            } else {
                c = rankEvaluations.compare(leftRow.get(), rightRow.get());
            }
            return c;
        }

        private HKey outputHKey(Row row)
        {
            HKey outputHKey = adapter().newHKey(outputHKeyTableRowType.hKey());
            row.hKey().copyTo(outputHKey);
            outputHKey.useSegments(outputHKeySegments);
            return outputHKey;
        }

        // Object state
        
        // Rows from each input stream are bound to the QueryContext. However, QueryContext doesn't use
        // ShareHolders, so they are needed here.

        private final Cursor leftInput;
        private final Cursor rightInput;
        private final ShareHolder<Row> leftRow = new ShareHolder<Row>();
        private final ShareHolder<Row> rightRow = new ShareHolder<Row>();
        private final RankEvaluations rankEvaluations;
        private final HKeyCache<HKey> hKeyCache;
        private HKey previousHKey;
        private boolean closed = true;
    }

    private interface RankExpressions {
        RankEvaluations buildEvaluations();
    }

    private interface RankEvaluations {
        long compare(Row left, Row right);
    }

    private static class OldRankExpressions implements RankExpressions {

        @Override
        public RankEvaluations buildEvaluations() {
            AbstractTwoArgExpressionEvaluation[] fieldRankingEvaluations
                    = new AbstractTwoArgExpressionEvaluation[fieldRankingExpressions.length];
            for (int f = 0; f < fieldRankingEvaluations.length; f++) {
                fieldRankingEvaluations[f] =
                        (AbstractTwoArgExpressionEvaluation) fieldRankingExpressions[f].evaluation();
            }
            return new OldRankEvaluations(fieldRankingEvaluations);
        }

        public OldRankExpressions(RowType leftRowType, RowType rightRowType, int leftOrderingFields,
                                  int rightOrderingFields, int comparisonFields) {
            fieldRankingExpressions = new RankExpression[comparisonFields];
            int leftField = leftRowType.nFields() - leftOrderingFields;
            int rightField = rightRowType.nFields() - rightOrderingFields;
            for (int f = 0; f < comparisonFields; f++) {
                fieldRankingExpressions[f] =
                        new RankExpression(new FieldExpression(leftRowType, leftField),
                                new FieldExpression(rightRowType, rightField));
                leftField++;
                rightField++;
            }
        }

        private final RankExpression[] fieldRankingExpressions;
    }

    private static class OldRankEvaluations implements RankEvaluations {

        private OldRankEvaluations(AbstractTwoArgExpressionEvaluation[] fieldRankingEvaluations) {
            this.fieldRankingEvaluations = fieldRankingEvaluations;
        }

        @Override
        public long compare(Row left, Row right) {
            long c = 0;
            for (AbstractTwoArgExpressionEvaluation fieldRankingEvaluation : fieldRankingEvaluations) {
                fieldRankingEvaluation.leftEvaluation().of(left);
                fieldRankingEvaluation.rightEvaluation().of(right);
                c = fieldRankingEvaluation.eval().getInt();
                if (c != 0) {
                    break;
                }
            }
            return c;
        }

        private final AbstractTwoArgExpressionEvaluation[] fieldRankingEvaluations;
    }
}
