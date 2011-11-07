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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.lang.Math.min;

public class BranchLookup_Default extends Operator
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

    // Operator interface

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
    }

    @Override
    public Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter, inputOperator.cursor(adapter));
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<Operator>(1);
        result.add(inputOperator);
        return result;
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // BranchLookup_Default interface

    public BranchLookup_Default(Operator inputOperator,
                                GroupTable groupTable,
                                RowType inputRowType,
                                RowType outputRowType,
                                API.LookupOption flag,
                                Limit limit)
    {
        ArgumentValidation.notNull("inputRowType", inputRowType);
        ArgumentValidation.notNull("outputRowType", outputRowType);
        ArgumentValidation.notNull("limit", limit);
        ArgumentValidation.isTrue("inputRowType instanceof IndexRowType || outputRowType != inputRowType",
                                  inputRowType instanceof IndexRowType || outputRowType != inputRowType);
        ArgumentValidation.isTrue("inputRowType instanceof UserTableRowType || !keepInput",
                                  inputRowType instanceof UserTableRowType || flag == API.LookupOption.DISCARD_INPUT);
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
        this.keepInput = flag == API.LookupOption.KEEP_INPUT;
        this.inputOperator = inputOperator;
        this.groupTable = groupTable;
        this.inputRowType = inputRowType;
        this.outputRowType = outputRowType;
        this.limit = limit;
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

    private static final Logger LOG = LoggerFactory.getLogger(BranchLookup_Default.class);

    // Object state

    private final Operator inputOperator;
    private final GroupTable groupTable;
    private final RowType inputRowType;
    private final RowType outputRowType;
    private final boolean keepInput;
    // If keepInput is true, inputPrecedesBranch controls whether input row appears before the retrieved branch.
    private final boolean inputPrecedesBranch;
    private final int commonSegments;
    private final int branchRootOrdinal;
    private final Limit limit;

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            inputCursor.open(bindings);
            advanceInput();
        }

        @Override
        public Row next()
        {
            checkQueryCancelation();
            Row nextRow = null;
            while (nextRow == null && inputRow.isHolding()) {
                switch (lookupState) {
                    case BEFORE:
                        if (keepInput && inputPrecedesBranch) {
                            nextRow = inputRow.get();
                        }
                        lookupState = LookupState.SCANNING;
                        break;
                    case SCANNING:
                        advanceLookup();
                        if (lookupRow.isHolding()) {
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
                LOG.debug("BranchLookup_Default: {}", lookupRow.get());
            }
            if (nextRow == null) {
                close();
            }
            return nextRow;
        }

        @Override
        public void close()
        {
            inputCursor.close();
            inputRow.release();
            lookupCursor.close();
            lookupRow.release();
        }

        // Execution interface

        Execution(StoreAdapter adapter, Cursor input)
        {
            super(adapter);
            this.inputCursor = input;
            this.lookupCursor = adapter.newGroupCursor(groupTable);
            this.lookupRowHKey = adapter.newHKey(outputRowType);
        }

        // For use by this class

        private void advanceLookup()
        {
            Row currentLookupRow;
            if ((currentLookupRow = lookupCursor.next()) != null) {
                if (limit.limitReached(currentLookupRow)) {
                    lookupState = LookupState.AFTER;
                    lookupRow.release();
                    close();
                } else {
                    currentLookupRow.runId(inputRow.get().runId());
                    lookupRow.hold(currentLookupRow);
                }
            } else {
                lookupState = LookupState.AFTER;
                lookupRow.release();
            }
        }

        private void advanceInput()
        {
            lookupState = LookupState.BEFORE;
            lookupRow.release();
            lookupCursor.close();
            Row currentInputRow = inputCursor.next();
            if (currentInputRow != null) {
                if (currentInputRow.rowType() == inputRowType) {
                    lookupRow.release();
                    computeLookupRowHKey(currentInputRow.hKey());
                    lookupCursor.rebind(lookupRowHKey, true);
                    lookupCursor.open(UndefBindings.only());
                }
                inputRow.hold(currentInputRow);
            } else {
                inputRow.release();
            }
        }

        private void computeLookupRowHKey(HKey inputRowHKey)
        {
            inputRowHKey.copyTo(lookupRowHKey);
            lookupRowHKey.useSegments(commonSegments);
            if (branchRootOrdinal != -1) {
                lookupRowHKey.extendWithOrdinal(branchRootOrdinal);
            }
        }

        // Object state

        private final Cursor inputCursor;
        private final ShareHolder<Row> inputRow = new ShareHolder<Row>();
        private final GroupCursor lookupCursor;
        private final ShareHolder<Row> lookupRow = new ShareHolder<Row>();
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
