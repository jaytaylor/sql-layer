/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.physicaloperator;

import com.akiban.ais.model.HKeySegment;
import com.akiban.qp.row.FlattenedRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.FlattenedRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.rowdata.RowDef;
import com.akiban.util.ArgumentValidation;

import java.util.*;

import static com.akiban.qp.physicaloperator.API.FlattenOption.*;

class Flatten_HKeyOrdered extends PhysicalOperator
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

    // PhysicalOperator interface

    @Override
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter, inputOperator.cursor(adapter));
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
    public List<PhysicalOperator> getInputOperators()
    {
        List<PhysicalOperator> result = new ArrayList<PhysicalOperator>(1);
        result.add(inputOperator);
        return result;
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Flatten_HKeyOrdered interface

    public Flatten_HKeyOrdered(PhysicalOperator inputOperator,
                               RowType parentType,
                               RowType childType,
                               API.JoinType joinType,
                               EnumSet<API.FlattenOption> options)
    {
        ArgumentValidation.notNull("parentType", parentType);
        ArgumentValidation.notNull("childType", childType);
        ArgumentValidation.notNull("flattenType", joinType);
        assert parentType != null;
        assert childType != null;
        this.inputOperator = inputOperator;
        this.parentType = parentType;
        this.childType = childType;
        this.flattenType = parentType.schema().newFlattenType(parentType, childType);
        boolean fullJoin = joinType.equals(API.JoinType.FULL_JOIN);
        this.leftJoin = fullJoin || joinType.equals(API.JoinType.LEFT_JOIN);
        this.rightJoin = fullJoin || joinType.equals(API.JoinType.RIGHT_JOIN);
        this.keepParent = options.contains(KEEP_PARENT);
        this.keepChild = options.contains(KEEP_CHILD);
        this.leftJoinShortensHKey = options.contains(LEFT_JOIN_SHORTENS_HKEY);
        if (this.leftJoinShortensHKey) {
            ArgumentValidation.isTrue("flags contains LEFT_JOIN_SHORTENS_HKEY but not LEFT_JOIN", leftJoin);
        }
        List<HKeySegment> childHKeySegments = childType.hKey().segments();
        HKeySegment lastChildHKeySegment = childHKeySegments.get(childHKeySegments.size() - 1);
        RowDef childRowDef = (RowDef) lastChildHKeySegment.table().rowDef();
        this.childOrdinal = childRowDef.getOrdinal();
        this.nChildHKeySegmentFields = lastChildHKeySegment.columns().size();
        this.parentHKeySegments = parentType.hKey().segments().size();
    }

    // Class state

    private static final int MAX_PENDING = 2;

    // Object state

    private final PhysicalOperator inputOperator;
    private final RowType parentType;
    private final RowType childType;
    private final FlattenedRowType flattenType;
    private final boolean leftJoin;
    private final boolean rightJoin;
    private final boolean keepParent;
    private final boolean keepChild;
    private final boolean leftJoinShortensHKey;
    // For constructing a left-join hkey
    private final int childOrdinal;
    private final int nChildHKeySegmentFields;
    private final int parentHKeySegments;

    // Inner classes

    private class Execution implements Cursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            input.open(bindings);
        }

        @Override
        public Row next()
        {
            Row outputRow = pending.take();
            Row inputRow;
            while (outputRow == null && (((inputRow = input.next()) != null) || parent.isNotNull())) {
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
                        if (parent.isNotNull() && parent.get().ancestorOf(inputRow)) {
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
            return outputRow;
        }

        @Override
        public void close()
        {
            parent.set(null);
            pending.clear();
            input.close();
        }

        // Execution interface

        Execution(StoreAdapter adapter, Cursor input)
        {
            this.adapter = adapter;
            this.input = input;
            this.leftJoinHKey = adapter.newHKey(childType);
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
                HKey hKey;
                if (leftJoinShortensHKey) {
                    hKey = parent.hKey();
                } else {
                    if (parent.hKey().segments() < parentHKeySegments) {
                        throw new IncompatibleRowException(
                            String.format("%s: parent hkey %s has been shortened by an earlier Flatten, " +
                                          "so this Flatten should specify LEFT_JOIN_SHORTENS_HKEY also",
                                          this, parent.hKey()));
                    }
                    // Copy leftJoinHKey to avoid aliasing problems. (leftJoinHKey changes on each parent row.)
                    hKey = adapter.newHKey(childType);
                    leftJoinHKey.copyTo(hKey);
                }
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
            parent.set(newParent);
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
            if (leftJoin && childlessParent && parent.isNotNull()) {
                if (inputRow == null) {
                    readyForLeftJoinRow = true;
                } else {
                    readyForLeftJoinRow =
                        // If the inputRow is a child of the parent then we don't need a left join row.
                        !(inputRow.rowType() == childType && parent.get().ancestorOf(inputRow)) &&
                        // Equality can occur when an input stream contains both flattened rows and the input
                        // to the flattening, e.g. an Order row, o, and a flatten(Customer, Order)
                        // row derived from o. Ordering is not well-defined in this case, so neither < nor <= is
                        // clearly preferable.
                        leftJoinHKey.compareTo(inputRow.hKey()) < 0;
                }
            }
            return readyForLeftJoinRow;
        }

        // Object state

        private final StoreAdapter adapter;
        private final Cursor input;
        private final RowHolder<Row> parent = new RowHolder<Row>();
        private final PendingRows pending = new PendingRows(MAX_PENDING);
        private final HKey leftJoinHKey;
        private boolean childlessParent;
    }
}
