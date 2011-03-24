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

import com.akiban.qp.Cursor;
import com.akiban.qp.row.*;
import com.akiban.qp.rowtype.FlattenedRowType;
import com.akiban.qp.rowtype.RowType;

import java.util.*;

public class Flatten_HKeyOrdered implements PhysicalOperator
{
    // PhysicalOperator interface

    public Cursor cursor()
    {
        return new Execution();
    }

    // Flatten_HKeyOrdered interface

    public FlattenedRowType rowType()
    {
        return flattenType;
    }

    public Flatten_HKeyOrdered(PhysicalOperator inputOperator, RowType parentType, RowType childType)
    {
        this(inputOperator, parentType, childType, 0);
    }

    public Flatten_HKeyOrdered(PhysicalOperator inputOperator, RowType parentType, RowType childType, int flags)
    {
        this.inputOperator = inputOperator;
        this.parentType = parentType;
        this.childType = childType;
        flattenType = parentType.schema().newFlattenType(parentType, childType);
        leftJoin = (flags & LEFT_JOIN) != 0;
        rightJoin = (flags & RIGHT_JOIN) != 0;
        keepParent = (flags & KEEP_PARENT) != 0;
        keepChild = (flags & KEEP_CHILD) != 0;
    }

    // Class state

    public static final int KEEP_PARENT = 0x01;
    public static final int KEEP_CHILD = 0x02;
    public static final int INNER_JOIN = 0x04;
    public static final int LEFT_JOIN = 0x08;
    public static final int RIGHT_JOIN = 0x10;
    static final int MAX_PENDING = 2;

    // Object state

    private final PhysicalOperator inputOperator;
    private final RowType parentType;
    private final RowType childType;
    private final FlattenedRowType flattenType;
    private final boolean leftJoin;
    private final boolean rightJoin;
    private final boolean keepParent;
    private final boolean keepChild;

    // Inner classes

    private class Execution extends SingleRowCachingCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            input.open();
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
                    generateLeftJoinRow(parent.managedRow());
                    parent.set(null);
                } else {
                    RowType inputRowType = input.rowType();
                    if (inputRowType == parentType) {
                        if (keepParent) {
                            addToPending();
                        }
                        if (parent.isNotNull()) {
                            // current parent row is childless, so it is left-join fodder.
                            generateLeftJoinRow(parent.managedRow());
                        }
                        parent.set(input.managedRow());
                    } else if (inputRowType == childType) {
                        if (keepChild) {
                            addToPending();
                        }
                        if (parent.isNotNull() && parent.ancestorOf(input)) {
                            // child is not an orphan
                            generateInnerJoinRow(parent.managedRow(), input.managedRow());
                        } else {
                            // child is an orphan
                            parent.set(null);
                            generateRightJoinRow(input.managedRow());
                        }
                    } else {
                        addToPending();
                        if (parentType.ancestorOf(inputRowType)) {
                            if (parent.isNotNull() && !parent.ancestorOf(input)) {
                                // We're past all descendents of the current parent
                                parent.set(null);
                            }
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

        Execution()
        {
            this.input = inputOperator.cursor();
        }

        // For use by this class

        private void generateInnerJoinRow(ManagedRow parent, ManagedRow child)
        {
            assert parent != null;
            assert child != null;
            pending.add(new FlattenedRow(flattenType, parent, child));
        }

        private void generateLeftJoinRow(ManagedRow parent)
        {
            assert parent != null;
            if (leftJoin) {
                pending.add(new FlattenedRow(flattenType, parent, null));
            }
        }

        private void generateRightJoinRow(ManagedRow child)
        {
            assert child != null;
            if (rightJoin) {
                pending.add(new FlattenedRow(flattenType, null, child));
            }
        }

        private void addToPending()
        {
            pending.add(input.currentRow());
        }

        // Object state

        private final Cursor input;
        private final RowHolder<ManagedRow> parent = new RowHolder<ManagedRow>();
        private final Pending pending = new Pending();
    }

    private static class Pending
    {
        public void add(Row row)
        {
            assert !full();
            row(end, row.managedRow());
            end = next(end);
        }

        public ManagedRow take()
        {
            ManagedRow row = null;
            if (!empty()) {
                row = row(start);
                row(start, null);
                start = next(start);
            }
            return row;
        }

        public void clear()
        {
            for (int i = 0; i <= MAX_PENDING; i++) {
                row(i, null);
            }
        }

        public Pending()
        {
            for (int i = 0; i <= MAX_PENDING; i++) {
                queue[i] = new RowHolder<ManagedRow>();
            }
            start = 0;
            end = 0;
        }

        // For use by this class

        private ManagedRow row(int i)
        {
            return ((RowHolder<ManagedRow>) queue[i]).managedRow();
        }

        private void row(int i, ManagedRow row)
        {
            ((RowHolder<ManagedRow>)queue[i]).set(row);
        }

        private boolean empty()
        {
            return end == start;
        }

        private boolean full()
        {
            return next(end) == start;
        }

        private int next(int x)
        {
            if (x++ == MAX_PENDING) {
                x = 0;
            }
            return x;
        }

        // Object state

        private final Object[] queue = new Object[MAX_PENDING + 1];
        private int start;
        private int end;
    }
}
