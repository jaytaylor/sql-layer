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
import com.akiban.qp.row.FlattenedRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ShareableRow;
import com.akiban.qp.rowtype.FlattenedRowType;
import com.akiban.qp.rowtype.RowType;

import java.util.LinkedList;
import java.util.Queue;

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
            outputRow(pending.poll());
            boolean moreInput;
            while (outputRow() == null && ((moreInput = input.next()) || parent != null)) {
                if (!moreInput) {
                    // child rows are processed immediately. parent rows are not,
                    // because when seen, we don't know if the next row will be another
                    // parent, a child, a row of child type that is not actually a child,
                    // a row of type other than parent or child, or end of stream. If we get
                    // here, then input is exhausted, and the only possibly remaining row would
                    // be due to a childless parent waiting to be processed.
                    generateLeftJoinRow(parent);
                    parent = null;
                } else {
                    RowType inputRowType = input.rowType();
                    if (inputRowType == parentType) {
                        if (keepParent) {
                            addToPending();
                        }
                        if (parent != null) {
                            // current parent row is childless, so it is left-join fodder.
                            generateLeftJoinRow(parent);
                        }
                        parent = input.currentRow();
                    } else if (inputRowType == childType) {
                        if (keepChild) {
                            addToPending();
                        }
                        if (parent != null && parent.ancestorOf(input)) {
                            // child is not an orphan
                            generateInnerJoinRow(parent, input.currentRow());
                        } else {
                            // child is an orphan
                            parent = null;
                            generateRightJoinRow(input.currentRow());
                        }
                    } else {
                        addToPending();
                        if (parentType.ancestorOf(inputRowType)) {
                            if (parent != null && !parent.ancestorOf(input)) {
                                // We're past all descendents of the current parent
                                parent = null;
                            }
                        }
                    }
                }
                outputRow(pending.poll());
            }
            return outputRow() != null;
        }

        @Override
        public void close()
        {
            input.close();
        }

        // Execution interface

        Execution()
        {
            this.input = inputOperator.cursor();
        }

        // For use by this class

        private void generateInnerJoinRow(Row parent, Row child)
        {
            assert parent != null;
            assert child != null;
            pending.add(new FlattenedRow(flattenType, parent, child));
        }

        private void generateLeftJoinRow(Row parent)
        {
            assert parent != null;
            if (leftJoin) {
                pending.add(new FlattenedRow(flattenType, parent, null));
            }
        }

        private void generateRightJoinRow(Row child)
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
        private Row parent;
        // TODO: LinkedList is not a good choice, as there is an object allocated per element added. JDK doesn't
        // TODO: seem to have an appropriate choice. Writing an array-based implementation is easy.
        private final Queue<Row> pending = new LinkedList<Row>();
    }
}
