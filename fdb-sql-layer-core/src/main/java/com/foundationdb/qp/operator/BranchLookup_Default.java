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

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.*;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.explain.std.LookUpOperatorExplainer;
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

 Given an index row or group row, BranchLookup_Default locates a
 related branch, i.e., a related row and all of its descendents.

 Unlike AncestorLookup, BranchLookup always retrieves a subtree under a
 targeted row.

 <h1>Arguments</h1>

 <ul>

 <li><b>GroupTable groupTable:</b> The group table containing the
 ancestors of interest.

 <li><b>RowType inputRowType:</b> Branches will be located for input
 rows of this type.

 <li><b>RowType outputRowType:</b> Type at the root of the branch to be
 retrieved.

 <li><b>API.InputPreservationOption flag:</b> Indicates whether rows of type rowType
 will be preserved in the output stream (flag = KEEP_INPUT), or
 discarded (flag = DISCARD_INPUT).

 <li><b>Limit limit (DEPRECATED):</b> A limit on the number of rows to
 be returned. The limit is specific to one Table. Deprecated
 because the result is not well-defined. In the case of a branching
 group, a limit on one sibling has impliciations on the return of rows
 of other siblings.

 </ul>

 inputRowType may be an index row type or a group row type. For a group
 row type, inputRowType must not match outputRowType. For an index row
 type, rowType may match outputRowType, and keepInput must be false
 (this may be relaxed in the future).

 The groupTable, inputRowType, and outputRowType must belong to the
 same group.

 If inputRowType is a table type, then inputRowType and outputRowType
 must be related in one of the following ways:

 <ul>

 <li>outputRowType is an ancestor of inputRowType.

 <li>outputRowType and inputRowType have a common ancestor, and
 outputRowType is a child of that common ancestor.

 </ul>

 If inputRowType is an index type, the above rules apply to the index's
 table's type.

 <h1>Behavior</h1>

 For each input row, the hkey is obtained. The hkey is transformed to
 yield an hkey that will locate the corresponding row of the output row
 type. Then the entire subtree under that hkey is retrieved. Orphan
 rows will be retrieved, even if there is no row of the outputRowType.

 All the retrieved records are written to the output stream in hkey
 order (ancestors before descendents), as is the input row if keepInput
 is true.

 If keepInput is true, then the input row appears either before all the
 rows of the branch or after all the rows of the branch. If
 outputRowType is an ancestor of inputRowType, then the input row is
 emitted after all the rows of the branch. Otherwise: inputRowType and
 outputRowType have some common ancestor, and outputRowType is the
 common ancestor's child. inputRowType has an ancestor, A, that is a
 different child of the common ancestor. The ordering is determined by
 comparing the ordinals of A and outputRowType.

 <h1>Output</h1>

 Nothing else to say.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 For each input row, BranchLookup_Default does one random access, and
 as many sequential accesses as are needed to retrieve the entire
 branch.

 <h1>Memory Requirements</h1>

 BranchLookup_Default stores two rows in memory.


 */

public class BranchLookup_Default extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s %s -> %s limit %s)",
                             getClass().getSimpleName(),
                             group.getRoot().getName(),
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
    public Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, inputOperator.cursor(context, bindingsCursor));
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<>(1);
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
                                Group group,
                                RowType inputRowType,
                                TableRowType outputRowType,
                                API.InputPreservationOption flag,
                                Limit limit)
    {
        ArgumentValidation.notNull("inputRowType", inputRowType);
        ArgumentValidation.notNull("outputRowType", outputRowType);
        ArgumentValidation.notNull("limit", limit);
        ArgumentValidation.isTrue("outputRowType != inputRowType", outputRowType != inputRowType);
        ArgumentValidation.isTrue("inputRowType instanceof TableRowType || flag == API.InputPreservationOption.DISCARD_INPUT",
                                  inputRowType instanceof TableRowType || flag == API.InputPreservationOption.DISCARD_INPUT);
        TableRowType inputTableType = null;
        if (inputRowType instanceof TableRowType) {
            inputTableType = (TableRowType) inputRowType;
        } else if (inputRowType instanceof IndexRowType) {
            inputTableType = ((IndexRowType) inputRowType).tableType();
        } else if (inputRowType instanceof HKeyRowType) {
            Schema schema = outputRowType.schema();
            inputTableType = schema.tableRowType(inputRowType.hKey().table());
        }
        assert inputTableType != null : inputRowType;
        Table inputTable = inputTableType.table();
        Table outputTable = outputRowType.table();
        ArgumentValidation.isSame("inputTable.getGroup()",
                                  inputTable.getGroup(),
                                  "outputTable.getGroup()",
                                  outputTable.getGroup());
        this.keepInput = flag == API.InputPreservationOption.KEEP_INPUT;
        this.inputOperator = inputOperator;
        this.group = group;
        this.inputRowType = inputRowType;
        this.outputRowType = outputRowType;
        this.limit = limit;
        this.commonAncestor = commonAncestor(inputTable, outputTable);
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
            // output type is ancestor of input row type
            this.inputPrecedesBranch = false;
        } else if (inputTable == commonAncestor) {
            // input row type is parent of output type
            this.inputPrecedesBranch = true;
        } else {
            // neither input type nor output type is the common ancestor
            Table ancestorOfInputAndChildOfCommon = inputTable;
            while (ancestorOfInputAndChildOfCommon.getParentTable() != commonAncestor) {
                ancestorOfInputAndChildOfCommon = ancestorOfInputAndChildOfCommon.getParentTable();
            }
            this.inputPrecedesBranch = ordinal(ancestorOfInputAndChildOfCommon) < branchRootOrdinal;
        }
    }

    // For use by this class

    private static Table commonAncestor(Table inputTable, Table outputTable)
    {
        int minLevel = min(inputTable.getDepth(), outputTable.getDepth());
        Table inputAncestor = inputTable;
        while (inputAncestor.getDepth() > minLevel) {
            inputAncestor = inputAncestor.getParentTable();
        }
        Table outputAncestor = outputTable;
        while (outputAncestor.getDepth() > minLevel) {
            outputAncestor = outputAncestor.getParentTable();
        }
        while (inputAncestor != outputAncestor) {
            inputAncestor = inputAncestor.getParentTable();
            outputAncestor = outputAncestor.getParentTable();
        }
        return outputAncestor;
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(BranchLookup_Default.class);
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: BranchLookup_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: BranchLookup_Default next");

    // Object state

    private final Operator inputOperator;
    private final Group group;
    private final RowType inputRowType;
    private final TableRowType outputRowType;
    private final boolean keepInput;
    // If keepInput is true, inputPrecedesBranch controls whether input row appears before the retrieved branch.
    private final boolean inputPrecedesBranch;
    private final Table commonAncestor;
    private final int branchRootOrdinal;
    private final Limit limit;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.OUTPUT_TYPE, outputRowType.getExplainer(context));
        TableRowType ancestorRowType = outputRowType.schema().tableRowType(commonAncestor);
        if ((ancestorRowType != inputRowType) && (ancestorRowType != outputRowType))
            atts.put(Label.ANCESTOR_TYPE, ancestorRowType.getExplainer(context));
        return new LookUpOperatorExplainer(getName(), atts, inputRowType, keepInput, inputOperator, context);
    }

    private class Execution extends ChainedCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                super.open();
                advanceInput();
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
                checkQueryCancelation();
                if (isIdle()) {
                    if (LOG_EXECUTION) {
                        LOG.debug("BranchLookup_Default: null");
                    }
                    return null;
                }
                Row nextRow = null;
                while (nextRow == null && inputRow != null) {
                    switch (lookupState) {
                        case BEFORE:
                            if (keepInput && inputPrecedesBranch) {
                                nextRow = inputRow;
                            }
                            lookupState = LookupState.SCANNING;
                            break;
                        case SCANNING:
                            advanceLookup();
                            if (lookupRow != null) {
                                nextRow = lookupRow;
                            }
                            break;
                        case AFTER:
                            if (keepInput && !inputPrecedesBranch) {
                                nextRow = inputRow;
                            }
                            advanceInput();
                            break;
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("BranchLookup_Default: {}", nextRow);
                }
                if (nextRow == null) {
                    setIdle();
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
            inputRow = null;
            lookupCursor.close();
            lookupRow = null;
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context, input);
            this.lookupCursor = adapter().newGroupCursor(group);
            this.lookupRowHKey = adapter().newHKey(outputRowType.hKey());
        }

        // For use by this class

        private void advanceLookup()
        {
            Row currentLookupRow;
            if ((currentLookupRow = lookupCursor.next()) != null) {
                if (limit.limitReached(currentLookupRow)) {
                    lookupState = LookupState.AFTER;
                    lookupRow = null;
                    close();
                } else {
                    lookupRow = currentLookupRow;
                }
            } else {
                lookupState = LookupState.AFTER;
                lookupRow = null;
            }
        }

        private void advanceInput()
        {
            lookupState = LookupState.BEFORE;
            lookupRow = null;
            lookupCursor.close();
            Row currentInputRow = input.next();
            if (currentInputRow != null) {
                if (currentInputRow.rowType() == inputRowType) {
                    lookupRow = null;
                    computeLookupRowHKey(currentInputRow);
                    lookupCursor.rebind(lookupRowHKey, true);
                    lookupCursor.open();
                }
                inputRow = currentInputRow;
            } else {
                inputRow = null;
            }
        }

        private void computeLookupRowHKey(Row row)
        {
            HKey ancestorHKey = row.ancestorHKey(commonAncestor);
            ancestorHKey.copyTo(lookupRowHKey);
            if (branchRootOrdinal != -1) {
                lookupRowHKey.extendWithOrdinal(branchRootOrdinal);
            }
        }

        // Object state

        private Row inputRow;
        private final GroupCursor lookupCursor;
        private Row lookupRow;
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
