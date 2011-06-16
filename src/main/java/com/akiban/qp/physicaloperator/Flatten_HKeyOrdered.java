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
import com.akiban.server.RowDef;
import com.akiban.util.ArgumentValidation;

import java.util.ArrayList;
import java.util.List;

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

    public FlattenedRowType rowType()
    {
        return flattenType;
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

    public Flatten_HKeyOrdered(PhysicalOperator inputOperator, RowType parentType, RowType childType, int flags)
    {
        ArgumentValidation.notNull("parentType", parentType);
        ArgumentValidation.notNull("childType", childType);
        ArgumentValidation.isTrue("(flags & (INNER_JOIN | LEFT_JOIN)) != (INNER_JOIN | LEFT_JOIN)",
                                  (flags & (INNER_JOIN | LEFT_JOIN)) != (INNER_JOIN | LEFT_JOIN));
        ArgumentValidation.isTrue("(flags & (INNER_JOIN | RIGHT_JOIN)) != (INNER_JOIN | RIGHT_JOIN)",
                                  (flags & (INNER_JOIN | RIGHT_JOIN)) != (INNER_JOIN | RIGHT_JOIN));
        assert parentType != null;
        assert childType != null;
        this.inputOperator = inputOperator;
        this.parentType = parentType;
        this.childType = childType;
        this.flattenType = parentType.schema().newFlattenType(parentType, childType);
        this.leftJoin = (flags & LEFT_JOIN) != 0;
        this.rightJoin = (flags & RIGHT_JOIN) != 0;
        this.keepParent = (flags & KEEP_PARENT) != 0;
        this.keepChild = (flags & KEEP_CHILD) != 0;
        this.leftJoinShortensHKey = (flags & LEFT_JOIN_SHORTENS_HKEY) != 0;
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

    public static final int DEFAULT = 0x00;
    public static final int KEEP_PARENT = 0x01;
    public static final int KEEP_CHILD = 0x02;
    public static final int INNER_JOIN = 0x04;
    public static final int LEFT_JOIN = 0x08;
    public static final int RIGHT_JOIN = 0x10;
    public static final int LEFT_JOIN_SHORTENS_HKEY = 0x20;
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

    private class Execution extends SingleRowCachingCursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            input.open(bindings);
        }

        @Override
        public boolean next()
        {
            outputRow(pending.take());
            boolean moreInput;
            while (outputRow() == null && ((moreInput = input.next()) || parent.isNotNull())) {
                if (!moreInput) {
                    // child rows are processed immediately. parent rows are not,
                    // because when seen, we don't know if the next row will be another
                    // parent, a child, a row of child type that is not actually a child,
                    // a row of type other than parent or child, or end of stream. If we get
                    // here, then input is exhausted, and the only possibly remaining row would
                    // be due to a childless parent waiting to be processed.
                    if (childlessParent) {
                        generateLeftJoinRow(parent.get());
                    }
                    parent.set(null);
                } else {
                    Row inputRow = input.currentRow();
                    RowType inputRowType = inputRow.rowType();
                    if (inputRowType == parentType) {
                        if (parent.isNotNull() && childlessParent) {
                            // current parent row is childless, so it is left-join fodder.
                            generateLeftJoinRow(parent.get());
                        }
                        if (keepParent) {
                            addToPending(inputRow);
                        }
                        parent.set(inputRow);
                        childlessParent = true;
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
                            parent.set(null);
                            generateRightJoinRow(inputRow);
                        }
                    } else {
                        addToPending(inputRow);
                        if (parent.isNotNull() && !parent.get().ancestorOf(inputRow)) {
                            // We're past all descendants of the current parent
                            if (childlessParent) {
                                generateLeftJoinRow(parent.get());
                            }
                            parent.set(null);
                        }
                    }
                }
                outputRow(pending.take());
            }
            return outputRow() != null;
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
                HKey hKey = leftJoinShortensHKey ? parent.hKey() : leftJoinHKey(parent.hKey());
                pending.add(new FlattenedRow(flattenType, parent, null, hKey));
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

        private HKey leftJoinHKey(HKey parentHKey)
        {
            if (parentHKey.segments() < parentHKeySegments) {
                throw new IncompatibleRowException(
                    String.format("%s: parent hkey %s has been shortened by an earlier Flatten, " +
                                  "so this Flatten should specify LEFT_JOIN_SHORTENS_HKEY also",
                                  this, parentHKey));
            }
            HKey childHKey = adapter.newHKey(childType);
            parentHKey.copyTo(childHKey);
            childHKey.extendWithOrdinal(childOrdinal);
            for (int i = 0; i < nChildHKeySegmentFields; i++) {
                childHKey.extendWithNull();
            }
            return childHKey;
        }

        // Object state

        private final StoreAdapter adapter;
        private final Cursor input;
        private final RowHolder<Row> parent = new RowHolder<Row>();
        private final PendingRows pending = new PendingRows(MAX_PENDING);
        private boolean childlessParent;
    }
}
