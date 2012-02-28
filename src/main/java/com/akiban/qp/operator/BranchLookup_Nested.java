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
import com.akiban.qp.rowtype.*;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static java.lang.Math.min;

/**

 <h1>Overview</h1>

 Given an index row or group row, BranchLookup_Default locates a
 related branch, i.e., a related row and all of its descendents.
 Branches are located 
 using the hkey in a row of the current query's QueryContext.

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

 <li><b>API.LookupOption flag:</b> Indicates whether rows of type rowType
 will be preserved in the output stream (flag = KEEP_INPUT), or
 discarded (flag = DISCARD_INPUT).

 <li><b>int inputBindingPosition:</b> Indicates input row's position in the query context. The hkey
 of this row will be used to locate ancestors.

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

 When this operator's cursor is opened, the row at position inputBindingPosition in the
 query context is accessed. The hkey from this row is obtained. The hkey is transformed to
 yield an hkey that will locate the corresponding row of the output row
 type. Then the entire subtree under that hkey is retrieved. Orphan
 rows will be retrieved, even if there is no row of the outputRowType.

 All the retrieved records are written to the output stream in hkey
 order (ancestors before descendents), as is the input row if KEEP_INPUT
 behavior is specified.

 If KEEP_INPUT is specified, then the input row appears either before all the
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

 For each input row, BranchLookup_Nested does one random access, and
 as many sequential accesses as are needed to retrieve the entire
 branch.

 <h1>Memory Requirements</h1>

 BranchLookup_Nested stores one row in memory.


 */

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
    public Cursor cursor(QueryContext context)
    {
        return new Execution(context);
    }

    @Override
    public String describePlan()
    {
        return toString();
    }

    // BranchLookup_Default interface

    public BranchLookup_Nested(GroupTable groupTable,
                               RowType inputRowType,
                               UserTableRowType outputRowType,
                               API.LookupOption flag,
                               int inputBindingPosition)
    {
        ArgumentValidation.notNull("groupTable", groupTable);
        ArgumentValidation.notNull("inputRowType", inputRowType);
        ArgumentValidation.notNull("outputRowType", outputRowType);
        ArgumentValidation.notNull("flag", flag);
        ArgumentValidation.isTrue("outputRowType != inputRowType", outputRowType != inputRowType);
        ArgumentValidation.isTrue("inputRowType instanceof UserTableRowType || flag == API.LookupOption.DISCARD_INPUT",
                                  inputRowType instanceof UserTableRowType || flag == API.LookupOption.DISCARD_INPUT);
        ArgumentValidation.isGTE("hKeyBindingPosition", inputBindingPosition, 0);
        UserTableRowType inputTableType = null;
        if (inputRowType instanceof UserTableRowType) {
            inputTableType = (UserTableRowType) inputRowType;
        } else if (inputRowType instanceof IndexRowType) {
            inputTableType = ((IndexRowType) inputRowType).tableType();
        } else if (inputRowType instanceof HKeyRowType) {
            Schema schema = outputRowType.schema();
            inputTableType = schema.userTableRowType(inputRowType.hKey().userTable());
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
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: BranchLookup_Nested open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: BranchLookup_Nested next");

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
        public void open()
        {
            TAP_OPEN.in();
            try {
                Row rowFromBindings = context.getRow(inputBindingPosition);
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
                cursor.open();
                inputRow.hold(rowFromBindings);
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            TAP_NEXT.in();
            try {
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
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close()
        {
            cursor.close();
        }

        // Execution interface

        Execution(QueryContext context)
        {
            super(context);
            this.cursor = adapter().newGroupCursor(groupTable);
            this.hKey = adapter().newHKey(outputRowType);
        }

        // Object state

        private final GroupCursor cursor;
        private final HKey hKey;
        private ShareHolder<Row> inputRow = new ShareHolder<Row>();
    }
}
