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

package com.akiban.qp.operator;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static java.lang.Math.min;

public class BranchLookup_Nested extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s %s -> %s)",
                             getClass().getSimpleName(),
                             groupTable.getName().getTableName(),
                             inputRowType,
                             outputRowType);
    }

    // Operator interface

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
    }

    @Override
    public Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter);
    }

    @Override
    public String describePlan()
    {
        return toString();
    }

    // BranchLookup_Default interface

    public BranchLookup_Nested(GroupTable groupTable,
                               RowType inputRowType,
                               RowType outputRowType,
                               API.LookupOption flag,
                               int inputBindingPosition)
    {
        ArgumentValidation.notNull("groupTable", groupTable);
        ArgumentValidation.notNull("inputRowType", inputRowType);
        ArgumentValidation.notNull("outputRowType", outputRowType);
        ArgumentValidation.notNull("flag", flag);
        ArgumentValidation.isTrue("inputRowType instanceof IndexRowType || outputRowType != inputRowType",
                                  inputRowType instanceof IndexRowType || outputRowType != inputRowType);
        ArgumentValidation.isTrue("inputRowType instanceof UserTableRowType || flag == API.LookupOption.DISCARD_INPUT",
                                  inputRowType instanceof UserTableRowType || flag == API.LookupOption.DISCARD_INPUT);
        ArgumentValidation.isGTE("hKeyBindingPosition", inputBindingPosition, 0);
        UserTableRowType inputTableType = null;
        if (inputRowType instanceof UserTableRowType) {
            inputTableType = (UserTableRowType) inputRowType;
        } else if (inputRowType instanceof IndexRowType) {
            inputTableType = ((IndexRowType) inputRowType).tableType();
        }
        assert inputTableType != null : inputRowType;
        UserTable inputTable = inputTableType.userTable();
        UserTable outputTable = outputRowType.userTable();
        ArgumentValidation.isSame("inputTable.getGroup()",
                                  inputTable.getGroup(),
                                  "outputTable.getGroup()",
                                  outputTable.getGroup());
        this.groupTable = groupTable;
        this.inputRowType = inputRowType;
        this.outputRowType = outputRowType;
        this.keepInput = flag == API.LookupOption.KEEP_INPUT;
        this.inputBindingPosition = inputBindingPosition;
        UserTable commonAncestor = commonAncestor(inputTable, outputTable);
        this.commonSegments = commonAncestor.getDepth() + 1;
        switch (outputTable.getDepth() - commonAncestor.getDepth()) {
            case 0:
                branchRootOrdinal = -1;
                break;
            case 1:
                branchRootOrdinal = ordinal(outputTable);
                break;
            default:
                branchRootOrdinal = -1;
                ArgumentValidation.isTrue("false", false);
                break;
        }
        // branchRootOrdinal = -1 means that outputTable is an ancestor of inputTable. In this case, inputPrecedesBranch
        // is false. Otherwise, branchRoot's parent is the common ancestor. Find inputTable's ancestor that is also
        // a child of the common ancestor. Then compare these ordinals to determine whether input precedes branch.
        if (this.branchRootOrdinal == -1) {
            this.inputPrecedesBranch = false;
        } else if (inputTable == commonAncestor) {
            this.inputPrecedesBranch = true;
        } else {
            UserTable ancestorOfInputAndChildOfCommon = inputTable;
            while (ancestorOfInputAndChildOfCommon.parentTable() != commonAncestor) {
                ancestorOfInputAndChildOfCommon = ancestorOfInputAndChildOfCommon.parentTable();
            }
            this.inputPrecedesBranch = ordinal(ancestorOfInputAndChildOfCommon) < branchRootOrdinal;
        }
    }

    // For use by this class

    private static UserTable commonAncestor(UserTable inputTable, UserTable outputTable)
    {
        int minLevel = min(inputTable.getDepth(), outputTable.getDepth());
        UserTable inputAncestor = inputTable;
        while (inputAncestor.getDepth() > minLevel) {
            inputAncestor = inputAncestor.parentTable();
        }
        UserTable outputAncestor = outputTable;
        while (outputAncestor.getDepth() > minLevel) {
            outputAncestor = outputAncestor.parentTable();
        }
        while (inputAncestor != outputAncestor) {
            inputAncestor = inputAncestor.parentTable();
            outputAncestor = outputAncestor.parentTable();
        }
        return outputAncestor;
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(BranchLookup_Nested.class);

    // Object state

    private final GroupTable groupTable;
    private final RowType inputRowType;
    private final RowType outputRowType;
    private final boolean keepInput;
    // If keepInput is true, inputPrecedesBranch controls whether input row appears before the retrieved branch.
    private final boolean inputPrecedesBranch;
    private final int inputBindingPosition;
    private final int commonSegments;
    private final int branchRootOrdinal;

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            Row rowFromBindings = (Row) bindings.get(inputBindingPosition);
            if (LOG.isDebugEnabled()) {
                LOG.debug("BranchLookup_Nested: open using {}", rowFromBindings);
            }
            assert rowFromBindings.rowType() == inputRowType : rowFromBindings;
            rowFromBindings.hKey().copyTo(hKey);
            hKey.useSegments(commonSegments);
            if (branchRootOrdinal != -1) {
                hKey.extendWithOrdinal(branchRootOrdinal);
            }
            cursor.rebind(hKey, true);
            cursor.open(bindings);
            inputRow.hold(rowFromBindings);
        }

        @Override
        public Row next()
        {
            checkQueryCancelation();
            Row row;
            if (keepInput && inputPrecedesBranch && inputRow.isHolding()) {
                row = inputRow.get();
                inputRow.release();
            } else {
                row = cursor.next();
                if (row == null) {
                    if (keepInput && !inputPrecedesBranch) {
                        assert inputRow.isHolding();
                        row = inputRow.get();
                        inputRow.release();
                    }
                    close();
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("BranchLookup_Nested: yield {}", row);
            }
            return row;
        }

        @Override
        public void close()
        {
            cursor.close();
        }

        // Execution interface

        Execution(StoreAdapter adapter)
        {
            super(adapter);
            this.cursor = adapter.newGroupCursor(groupTable);
            this.hKey = adapter.newHKey(outputRowType);
        }

        // Object state

        private final GroupCursor cursor;
        private final HKey hKey;
        private ShareHolder<Row> inputRow = new ShareHolder<Row>();
    }
}
