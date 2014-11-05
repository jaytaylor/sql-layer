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

import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.HKeyRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TClass;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
<li><b>TableRowType outputHKeyTableRowType:</b> Before eliminating duplicate hkeys, hkeys from the input stream
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
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, bindingsCursor);
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
        List<Operator> result = new ArrayList<>(2);
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
                             TableRowType outputHKeyTableRowType)
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
        com.foundationdb.ais.model.HKey outputHKeyDefinition = outputHKeyTableRowType.table().hKey();
        this.outputHKeyRowType = outputHKeyTableRowType.schema().newHKeyRowType(outputHKeyDefinition);
        this.outputHKeySegments = outputHKeyDefinition.segments().size();
        // Setup for row comparisons
        RowsComparator comparator = newComparator;
        fieldRankingExpressions = new RankExpressions(
                leftRowType, rightRowType,
                leftOrderingFields, rightOrderingFields,
                comparisonFields,
                comparator);
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: HKeyUnion_Ordered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: HKeyUnion_Ordered next");
    private static final Logger LOG = LoggerFactory.getLogger(HKeyUnion_Ordered.class);

    // Object state

    private final Operator left;
    private final Operator right;
    private final RankExpressions fieldRankingExpressions;
    private final boolean advanceLeftOnMatch;
    private final boolean advanceRightOnMatch;
    private final TableRowType outputHKeyTableRowType;
    private final HKeyRowType outputHKeyRowType;
    private final int outputHKeySegments;
    private final int leftFields;
    private final int rightFields;
    private final int compareFields;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.NUM_SKIP, PrimitiveExplainer.getInstance(leftFields));
        atts.put(Label.NUM_SKIP, PrimitiveExplainer.getInstance(rightFields));
        atts.put(Label.NUM_COMPARE, PrimitiveExplainer.getInstance(compareFields));
        atts.put(Label.OUTPUT_TYPE, outputHKeyTableRowType.getExplainer(context));
        return new CompoundExplainer(Type.ORDERED, atts);
    }

    // Inner classes

    private class Execution extends MultiChainedCursor 
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                super.open();
                previousHKey = null;
                nextLeftRow();
                nextRightRow();
                if (leftRow == null && rightRow == null)
                    setIdle();
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
                Row nextRow = null;
                while (isActive() && nextRow == null) {
                    assert !(leftRow == null && rightRow == null);
                    long c = compareRows();
                    if (c < 0) {
                        nextRow = leftRow;
                        nextLeftRow();
                    } else if (c > 0) {
                        nextRow = rightRow;
                        nextRightRow();
                    } else {
                        nextRow = leftRow;
                        if (advanceLeftOnMatch) {
                            nextLeftRow();
                        }
                        if (advanceRightOnMatch) {
                            nextRightRow();
                        }
                    }
                    if (leftRow == null && rightRow == null) {
                        setIdle();
                    }
                    if (nextRow == null) {
                        setIdle();
                    } else if (previousHKey == null || !previousHKey.prefixOf(nextRow.hKey())) {
                        HKey nextHKey = outputHKey(nextRow);
                        nextRow = (Row) nextHKey;
                        previousHKey = nextHKey;
                    } else {
                        nextRow = null;
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("HKeyUnion_Ordered: yield {}", nextRow);
                }
                return nextRow;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }
        
        @Override
        public void close()
        {
            super.close();
            leftRow = null;
            rightRow = null;
        }

        @Override
        protected Operator left() {
            return left;
        }
        
        @Override 
        protected Operator right() {
            return right;
        }
        // Execution interface

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor)
        {
            super(context, bindingsCursor);
        }
        
        // For use by this class
        
        private void nextLeftRow()
        {
            Row row;
            do {
                row = leftInput.next();
            } while (row != null && previousHKey != null && previousHKey.prefixOf(row.hKey()));
            leftRow = row;
        }
        
        private void nextRightRow()
        {
            Row row;
            do {
                row = rightInput.next();
            } while (row != null && previousHKey != null && previousHKey.prefixOf(row.hKey()));
            rightRow = row;
        }
        
        private long compareRows()
        {
            long c;
            assert !isClosed();
            assert !(leftRow == null && rightRow == null);
            if (leftRow == null) {
                c = 1;
            } else if (rightRow == null) {
                c = -1;
            } else {
                c = fieldRankingExpressions.compare(leftRow, rightRow);
            }
            return c;
        }

        private HKey outputHKey(Row row)
        {
            HKey outputHKey = adapter().getKeyCreator().newHKey(outputHKeyTableRowType.hKey());
            row.hKey().copyTo(outputHKey);
            outputHKey.useSegments(outputHKeySegments);
            return outputHKey;
        }

        // Object state

        private Row leftRow;
        private Row rightRow;
        private HKey previousHKey;
    }

    private interface RowsComparator {
        public abstract int compare(Row left, Row right, int leftIndex, int rightIndex);
    }

    private static final RowsComparator newComparator = new RowsComparator() {
        @Override
        public int compare(Row left, Row right, int leftIndex, int rightIndex) {
            return TClass.compare(
                    left.rowType().typeAt(leftIndex), left.value(leftIndex),
                    right.rowType().typeAt(rightIndex), right.value(rightIndex));
        }
    };

    private static class RankExpressions {

        private RankExpressions(RowType leftRowType, RowType rightRowType, int leftOrderingFields,
                                   int rightOrderingFields, int comparisonFields,
                                   RowsComparator comparator)
        {
            this.leftOffset = leftRowType.nFields() - leftOrderingFields;
            this.rightOffset = rightRowType.nFields() - rightOrderingFields;
            this.comparisonExpressions = comparisonFields;
            this.comparator = comparator;
        }

        public long compare(Row left, Row right) {
            int c = 0;
            int leftIndex = leftOffset;
            int rightIndex = rightOffset;
            for (int i = 0; i < comparisonExpressions; ++i) {
                c = comparator.compare(left, right, leftIndex, rightIndex);
                if (c != 0)
                    break;
                ++leftIndex;
                ++rightIndex;
            }
            return c;
        }

        private final int leftOffset;
        private final int rightOffset;
        private final int comparisonExpressions;
        private final RowsComparator comparator;
    }
}
