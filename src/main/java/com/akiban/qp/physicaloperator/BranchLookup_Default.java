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

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowHolder;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.RowDef;
import com.akiban.util.ArgumentValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.min;

public class BranchLookup_Default extends PhysicalOperator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s %s -> %s limit %s)",
                             getClass().getSimpleName(),
                             groupTable.getName().getTableName(),
                             inputRowType,
                             outputRowType,
                             limit);
    }

    // PhysicalOperator interface

    @Override
    public Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter, inputOperator.cursor(adapter));
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

    // BranchLookup_Default interface

    public BranchLookup_Default(PhysicalOperator inputOperator,
                                GroupTable groupTable,
                                RowType inputRowType,
                                RowType outputRowType,
                                boolean keepInput,
                                Limit limit)
    {
        ArgumentValidation.notNull("inputRowType", inputRowType);
        ArgumentValidation.notNull("outputRowType", outputRowType);
        ArgumentValidation.notNull("limit", limit);
        ArgumentValidation.isTrue("inputRowType instanceof IndexRowType || outputRowType != inputRowType",
                                  inputRowType instanceof IndexRowType || outputRowType != inputRowType);
        ArgumentValidation.isTrue("inputRowType instanceof UserTableRowType || !keepInput",
                                  inputRowType instanceof UserTableRowType || !keepInput);
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
        this.keepInput = keepInput;
        this.inputOperator = inputOperator;
        this.groupTable = groupTable;
        this.inputRowType = inputRowType;
        this.outputRowType = outputRowType;
        this.limit = limit;
        UserTable commonAncestor = commonAncestor(inputTable, outputTable);
        switch (outputTable.getDepth() - commonAncestor.getDepth()) {
            case 0:
                this.branchRoot = null;
                break;
            case 1:
                this.branchRoot = outputTable;
                break;
            default:
                this.branchRoot = null;
                ArgumentValidation.isTrue("false", false);
        }
        this.commonSegments = commonAncestor.getDepth() + 1;
        // branchRoot is null means that outputTable is an ancestor of inputTable. In this case, inputPrecedesBranch
        // is false. Otherwise, branchRoot's parent is the common ancestor. Find inputTable's ancestor that is also
        // a child of the common ancestor. Then compare these ordinals to determine whether input precedes branch.
        if (this.branchRoot == null) {
            this.inputPrecedesBranch = false;
        } else {
            UserTable ancestorOfInputAndChildOfCommon = inputTable;
            while (ancestorOfInputAndChildOfCommon.parentTable() != commonAncestor) {
                ancestorOfInputAndChildOfCommon = ancestorOfInputAndChildOfCommon.parentTable();
            }
            this.inputPrecedesBranch = ordinal(ancestorOfInputAndChildOfCommon) < ordinal(branchRoot);
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

    private int ordinal(UserTable table)
    {
        return ((RowDef) table.rowDef()).getOrdinal();
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(BranchLookup_Default.class);

    // Object state

    private final PhysicalOperator inputOperator;
    private final GroupTable groupTable;
    private final RowType inputRowType;
    private final RowType outputRowType;
    private final boolean keepInput;
    // If keepInput is true, inputPrecedesBranch controls whether input row appears before the retrieved branch.
    private final boolean inputPrecedesBranch;
    private final int commonSegments;
    private final UserTable branchRoot;
    private final Limit limit;

    private class Execution extends SingleRowCachingCursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            inputCursor.open(bindings);
            advanceInput();
        }

        @Override
        public boolean booleanNext()
        {
            assert false;
            return false;
        }

        @Override
        public Row next()
        {
            Row nextRow = null;
            while (nextRow == null && inputRow.isNotNull()) {
                switch (lookupState) {
                    case BEFORE:
                        if (keepInput && inputPrecedesBranch) {
                            nextRow = inputRow.get();
                        }
                        lookupState = LookupState.SCANNING;
                        break;
                    case SCANNING:
                        advanceLookup();
                        if (lookupRow.isNotNull()) {
                            nextRow = lookupRow.get();
                        }
                        break;
                    case AFTER:
                        if (keepInput && !inputPrecedesBranch) {
                            nextRow = inputRow.get();
                        }
                        advanceInput();
                        break;
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Lookup: {}", lookupRow.isNull() ? null : lookupRow.get());
            }
            return nextRow;
        }

        @Override
        public void close()
        {
            outputRow(null);
            inputCursor.close();
            inputRow.set(null);
            lookupCursor.close();
            lookupRow.set(null);
        }

        // Execution interface

        Execution(StoreAdapter adapter, Cursor input)
        {
            this.inputCursor = input;
            this.lookupCursor = adapter.newGroupCursor(groupTable);
            this.lookupRowHKey = adapter.newHKey(outputRowType);
        }

        // For use by this class

        private void advanceLookup()
        {
            Row currentRow;
            if ((currentRow = lookupCursor.next()) != null) {
                if (limit.limitReached(currentRow)) {
                    lookupState = LookupState.AFTER;
                    lookupRow.set(null);
                    close();
                } else {
                    lookupRow.set(currentRow);
                }
            } else {
                lookupState = LookupState.AFTER;
                lookupRow.set(null);
            }
        }

        private void advanceInput()
        {
            lookupState = LookupState.BEFORE;
            lookupRow.set(null);
            lookupCursor.close();
            Row currentInputRow = inputCursor.next();
            if (currentInputRow != null) {
                if (currentInputRow.rowType() == inputRowType) {
                    lookupRow.set(null);
                    computeLookupRowHKey(currentInputRow.hKey());
                    lookupCursor.rebind(lookupRowHKey, true);
                    lookupCursor.open(UndefBindings.only());
                }
                inputRow.set(currentInputRow);
            } else {
                inputRow.set(null);
            }
        }

        private void computeLookupRowHKey(HKey inputRowHKey)
        {
            inputRowHKey.copyTo(lookupRowHKey);
            lookupRowHKey.useSegments(commonSegments);
            if (branchRoot != null) {
                lookupRowHKey.extend(branchRoot);
            }
        }

        // Object state

        private final Cursor inputCursor;
        private final RowHolder<Row> inputRow = new RowHolder<Row>();
        private final GroupCursor lookupCursor;
        private final RowHolder<Row> lookupRow = new RowHolder<Row>();
        private final HKey lookupRowHKey;
        private LookupState lookupState;
    }

    // Inner classes

    private static enum LookupState
    {
        // Before retrieving the first lookup row for the current input row.
        BEFORE,
        // After the first lookup row has been retrieved, and before we have discovered that there are no more
        // lookup rows.
        SCANNING,
        // After the lookup rows for the current input row have all been scanned, (known because lookupCursor.booleanNext
        // returned false).
        AFTER
    }
}
