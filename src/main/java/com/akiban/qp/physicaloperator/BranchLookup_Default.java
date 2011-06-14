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
                branchRoot = null;
                break;
            case 1:
                branchRoot = outputTable;
                break;
            default:
                branchRoot = null;
                ArgumentValidation.isTrue("false", false);
        }
        this.commonSegments = commonAncestor.getDepth() + 1;
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

    private static final Logger LOG = LoggerFactory.getLogger(BranchLookup_Default.class);

    // Object state

    private final PhysicalOperator inputOperator;
    private final GroupTable groupTable;
    private final RowType inputRowType;
    private final RowType outputRowType;
    private final boolean keepInput;
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
        public boolean next()
        {
            Row nextRow = null;
            while (nextRow == null && inputRow.isNotNull()) {
                switch (lookupState) {
                    case BEFORE:
                        // Input row shows up before lookup rows. That might not be strictly in hkey order, but it
                        // shouldn't matter because outputRowType is not an ancestor of inputRowType.
                        if (keepInput) {
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
                        advanceInput();
                        break;
                }
            }
            outputRow(nextRow);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Lookup: {}", lookupRow.isNull() ? null : lookupRow.get());
            }
            return nextRow != null;
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
            if (lookupCursor.next()) {
                Row currentLookupRow = lookupCursor.currentRow();
                if (currentLookupRow == null) {
                    lookupState = LookupState.AFTER;
                    lookupRow.set(null);
                } else {
                    if (limit.limitReached(currentLookupRow)) {
                        lookupState = LookupState.AFTER;
                        lookupRow.set(null);
                        close();
                    } else {
                        currentLookupRow.runId(inputRow.get().runId());
                        lookupRow.set(currentLookupRow);
                    }
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
            if (inputCursor.next()) {
                Row currentInputRow = inputCursor.currentRow();
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
        // After the lookup rows for the current input row have all been scanned, (known because lookupCursor.next
        // returned false).
        AFTER
    }
}
