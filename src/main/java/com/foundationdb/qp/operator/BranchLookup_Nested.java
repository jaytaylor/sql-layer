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
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.rowtype.*;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.explain.std.LookUpOperatorExplainer;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static java.lang.Math.min;

/**

 <h1>Overview</h1>

 Given an index row or group row, BranchLookup_Nested locates a
 related branch, i.e., a related row and all of its descendents.
 Branches are located 
 using the hkey in a row of the current query's QueryContext.

 Unlike AncestorLookup, BranchLookup always retrieves a subtree under a
 targeted row.

 <h1>Arguments</h1>

 <ul>

 <li><b>Group group:</b> The group containing the ancestors of interest.

 <li><b>RowType inputRowType:</b> Bound row will be of this type.
 
 <li><b>RowType sourceRowType:</b> Branches will be located for input
 rows of this type. Possibly a subrow of inputRowType.
 
 <li><b>TableRowType ancestorRowType:</b> Identifies the table in the group at which branching occurs.
 Must be an ancestor of both inputRowType's table and outputRowTypes' tables.

 <li><b>TableRowType outputRowTypes:</b> Types within the branch to be
 retrieved.

 <li><b>API.InputPreservationOption flag:</b> Indicates whether rows of type rowType
 will be preserved in the output stream (flag = KEEP_INPUT), or
 discarded (flag = DISCARD_INPUT).

 <li><b>int inputBindingPosition:</b> Indicates input row's position in the query context. The hkey
 of this row will be used to locate ancestors.

 <li><b>int lookaheadQuantum:</b> Number of cursors to try to keep open by looking
  ahead in bindings stream.

 </ul>

 inputRowType may be an index row type, a user table row type, or an hkey row type. flag = KEEP_INPUT is permitted
 only for user table row types.

 The groupTable, inputRowType, and outputRowTypes must belong to the
 same group.
 
 ancestorRowType's table must be an ancestor of
 inputRowType's table and outputRowTypes' tables.

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
                             group.getRoot().getName(),
                             sourceRowType,
                             outputRowTypes);
    }

    // Operator interface

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
    }

    @Override
    public Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        if (lookaheadQuantum <= 1) {
            return new Execution(context, bindingsCursor);
        }
        else {
            return new LookaheadExecution(context, bindingsCursor, 
                                          context.getStore(commonAncestor),
                                          lookaheadQuantum);
        }
    }

    @Override
    public String describePlan()
    {
        return toString();
    }

    // BranchLookup_Nested interface

    public BranchLookup_Nested(Group group,
                               RowType inputRowType,
                               RowType sourceRowType,
                               TableRowType ancestorRowType,
                               Collection<TableRowType> outputRowTypes,
                               API.InputPreservationOption flag,
                               int inputBindingPosition,
                               int lookaheadQuantum)
    {
        ArgumentValidation.notNull("group", group);
        ArgumentValidation.notNull("inputRowType", inputRowType);
        ArgumentValidation.notNull("sourceRowType", sourceRowType);
        ArgumentValidation.notEmpty("outputRowTypes", outputRowTypes);
        ArgumentValidation.notNull("flag", flag);
        ArgumentValidation.isTrue("sourceRowType instanceof TableRowType || flag == API.InputPreservationOption.DISCARD_INPUT",
                                  sourceRowType instanceof TableRowType || flag == API.InputPreservationOption.DISCARD_INPUT);
        ArgumentValidation.isGTE("hKeyBindingPosition", inputBindingPosition, 0);
        TableRowType inputTableType = null;
        if (sourceRowType instanceof TableRowType) {
            inputTableType = (TableRowType) sourceRowType;
        } else if (sourceRowType instanceof IndexRowType) {
            inputTableType = ((IndexRowType) sourceRowType).tableType();
        } else if (sourceRowType instanceof HKeyRowType) {
            Schema schema = outputRowTypes.iterator().next().schema();
            inputTableType = schema.tableRowType(sourceRowType.hKey().table());
        }
        assert inputTableType != null : sourceRowType;
        Table inputTable = inputTableType.table();
        ArgumentValidation.isSame("inputTable.getGroup()", inputTable.getGroup(), 
                                  "group", group);
        Table commonAncestor;
        if (ancestorRowType == null) {
            commonAncestor = inputTable;
        } else {
            commonAncestor = ancestorRowType.table();
            ArgumentValidation.isTrue("ancestorRowType.ancestorOf(inputTableType)",
                                      ancestorRowType.ancestorOf(inputTableType));
        }
        for (TableRowType outputRowType : outputRowTypes) {
            Table outputTable = outputRowType.table();
            ArgumentValidation.isSame("outputTable.getGroup()", outputTable.getGroup(), 
                                      "group", group);
            if (ancestorRowType == null) {
                commonAncestor = commonAncestor(commonAncestor, outputTable);
            }
            else {
                ArgumentValidation.isTrue("ancestorRowType.ancestorOf(outputRowType)",
                                          ancestorRowType.ancestorOf(outputRowType));
            }
        }
        this.group = group;
        this.inputRowType = inputRowType;
        this.sourceRowType = sourceRowType;
        this.outputRowTypes = new ArrayList<>(outputRowTypes);
        Collections.sort(this.outputRowTypes, 
                         new Comparator<TableRowType>()
                         {
                             @Override
                             public int compare(TableRowType x, TableRowType y)
                                 {
                                     return x.table().getDepth() - y.table().getDepth();
                                 }
                         });
        this.commonAncestor = commonAncestor;
        this.keepInput = flag == API.InputPreservationOption.KEEP_INPUT;
        this.inputBindingPosition = inputBindingPosition;
        this.lookaheadQuantum = lookaheadQuantum;
        // See whether there is a single branch beneath commonAncestor
        // with all output row types.
        Table outputTable = this.outputRowTypes.get(0).table();
        boolean allOneBranch;
        if (outputTable == commonAncestor) {
            allOneBranch = false;
        }
        else {
            while (outputTable.getParentTable() != commonAncestor) {
                outputTable = outputTable.getParentTable();
            }
            TableRowType outputTableRowType = this.outputRowTypes.get(0).schema().tableRowType(outputTable);
            allOneBranch = true;
            for (int i = 1; i < this.outputRowTypes.size(); i++) {
                if (!outputTableRowType.ancestorOf(this.outputRowTypes.get(i))) {
                    allOneBranch = false;
                    break;
                }
            }
        }
        if (allOneBranch) {
            branchRootOrdinal = ordinal(outputTable);
        }
        else {
            branchRootOrdinal = -1;
        }
        // branchRootOrdinal = -1 means that outputTable is an ancestor of inputTable. In this case, inputPrecedesBranch
        // is false. Otherwise, branchRoot's parent is the common ancestor. Find inputTable's ancestor that is also
        // a child of the common ancestor. Then compare these ordinals to determine whether input precedes branch.
        if (this.branchRootOrdinal == -1) {
            this.inputPrecedesBranch = false;
        } else if (inputTable == commonAncestor) {
            this.inputPrecedesBranch = true;
        } else {
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

    private static final Logger LOG = LoggerFactory.getLogger(BranchLookup_Nested.class);
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: BranchLookup_Nested open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: BranchLookup_Nested next");

    // Object state

    private final Group group;
    private final RowType inputRowType, sourceRowType;
    private final List<TableRowType> outputRowTypes;
    private final boolean keepInput;
    // If keepInput is true, inputPrecedesBranch controls whether input row appears before the retrieved branch.
    private final boolean inputPrecedesBranch;
    private final int inputBindingPosition;
    private final int lookaheadQuantum;
    private final Table commonAncestor;
    private final int branchRootOrdinal;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(inputBindingPosition));
        for (TableRowType outputRowType : outputRowTypes) {
            atts.put(Label.OUTPUT_TYPE, outputRowType.getExplainer(context));
        }
        TableRowType outputRowType = outputRowTypes.get(0);
        TableRowType ancestorRowType = outputRowType.schema().tableRowType(commonAncestor);
        if ((ancestorRowType != sourceRowType) && (ancestorRowType != outputRowType)) {
            atts.put(Label.ANCESTOR_TYPE, ancestorRowType.getExplainer(context));
        }
        atts.put(Label.PIPELINE, PrimitiveExplainer.getInstance(lookaheadQuantum));
        return new LookUpOperatorExplainer(getName(), atts, sourceRowType, false, null, context);
    }

    // Inner classes

    private class Execution extends LeafCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                Row rowFromBindings = bindings.getRow(inputBindingPosition);
                assert rowFromBindings.rowType() == inputRowType : rowFromBindings;
                if (inputRowType != sourceRowType) {
                    rowFromBindings = rowFromBindings.subRow(sourceRowType);
                }
                if (LOG_EXECUTION) {
                    LOG.debug("BranchLookup_Nested: open using {}", rowFromBindings);
                }
                computeLookupRowHKey(rowFromBindings);
                cursor.rebind(hKey, true);
                cursor.open();
                inputRow = rowFromBindings;
                idle = false;
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
                Row row;
                if (keepInput && inputPrecedesBranch && inputRow != null) {
                    row = inputRow;
                    inputRow = null;
                } else {
                    do {
                        row = cursor.next();
                    } while ((row != null) && !outputRowTypes.contains(row.rowType()));
                    if (row == null) {
                        if (keepInput && !inputPrecedesBranch) {
                            assert inputRow != null;
                            row = inputRow;
                            inputRow = null;
                        }
                        close();
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("BranchLookup_Nested: yield {}", row);
                }
                idle = row == null;
                return row;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        @Override
        public void close()
        {
            CursorLifecycle.checkIdleOrActive(this);
            cursor.close();
            state = CursorLifecycle.CursorState.CLOSED;
        }

        @Override
        public boolean isIdle()
        {
            return idle;
        }

        @Override
        public boolean isActive()
        {
            return !idle;
        }

        @Override
        public boolean isClosed()
        {
            return cursor.isClosed();
        }

        // Execution interface

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor)
        {
            super(context, bindingsCursor);
            this.cursor = adapter().newGroupCursor(group);
            this.hKey = adapter().newHKey(outputRowTypes.get(0).hKey());
        }

        // For use by this class

        private void computeLookupRowHKey(Row row)
        {
            HKey ancestorHKey = row.ancestorHKey(commonAncestor);
            ancestorHKey.copyTo(hKey);
            if (branchRootOrdinal != -1) {
                hKey.extendWithOrdinal(branchRootOrdinal);
            }
        }

        // Object state

        private final GroupCursor cursor;
        private final HKey hKey;
        private Row inputRow;
        private boolean idle = true;
    }

    private class BranchCursor implements BindingsAwareCursor
    {
        // BindingsAwareCursor interface

        @Override
        public void open() {
            Row rowFromBindings = bindings.getRow(inputBindingPosition);
            assert rowFromBindings.rowType() == inputRowType : rowFromBindings;
            if (inputRowType != sourceRowType) {
                rowFromBindings = rowFromBindings.subRow(sourceRowType);
            }
            computeLookupRowHKey(rowFromBindings);
            cursor.rebind(hKey, true);
            cursor.open();
            inputRow = rowFromBindings;
        }

        @Override
        public Row next() {
            Row row = null;
            if (keepInput && inputPrecedesBranch && inputRow != null) {
                row = inputRow;
                inputRow = null;
            } else {
                do {
                    row = cursor.next();
                } while ((row != null) && !outputRowTypes.contains(row.rowType()));
                if (row == null) {
                    if (keepInput && !inputPrecedesBranch) {
                        assert inputRow != null;
                        row = inputRow;
                        inputRow = null;
                    }
                    close();
                }
            }
            if (ExecutionBase.LOG_EXECUTION) {
                LOG.debug("BranchLookup#BranchCursor yield: {} ", row );
            }
            return row;
        }

        @Override
        public void jump(Row row, ColumnSelector columnSelector) {
            cursor.jump(row, columnSelector);
        }

        @Override
        public void close() {
            inputRow = null;
            cursor.close();
        }

        @Override
        public void destroy() {
            close();
            cursor.destroy();
        }

        @Override
        public boolean isIdle() {
            return cursor.isIdle();
        }

        @Override
        public boolean isActive() {
            return cursor.isActive();
        }

        @Override
        public boolean isDestroyed() {
            return cursor.isDestroyed();
        }
        
        @Override
        public void rebind(QueryBindings bindings) {
            this.bindings = bindings;
        }

        // BranchCursor interface
        public BranchCursor(StoreAdapter adapter) {
            this.cursor = adapter.newGroupCursor(group);
            this.hKey = adapter.newHKey(outputRowTypes.get(0).hKey());
        }

        // For use by this class

        private void computeLookupRowHKey(Row row)
        {
            HKey ancestorHKey = row.ancestorHKey(commonAncestor);
            ancestorHKey.copyTo(hKey);
            if (branchRootOrdinal != -1) {
                hKey.extendWithOrdinal(branchRootOrdinal);
            }
        }

        // Object state

        private final GroupCursor cursor;
        private final HKey hKey;
        private Row inputRow;
        private QueryBindings bindings;
    }

    private class LookaheadExecution extends LookaheadLeafCursor<BranchCursor>
    {
        // Cursor interface

        @Override
        public void open() {
            TAP_OPEN.in();
            try {
                super.open();
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next() {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                Row row = super.next();
                if (LOG_EXECUTION) {
                    LOG.debug("BranchLookup_Nested: yield {}", row);
                }
                return row;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        // LookaheadLeafCursor interface

        @Override
        protected BranchCursor newCursor(QueryContext context, StoreAdapter adapter) {
            return new BranchCursor(adapter);
        }

        @Override
        protected BranchCursor openACursor(QueryBindings bindings, boolean lookahead) {
            BranchCursor cursor = super.openACursor(bindings, lookahead);
            if (LOG_EXECUTION) {
                LOG.debug("BranchLookup_Nested: open{} using {}", lookahead ? " lookahead" : "", cursor.inputRow);
            }
            return cursor;
        }
        
        // LookaheadExecution interface

        LookaheadExecution(QueryContext context, QueryBindingsCursor bindingsCursor, 
                           StoreAdapter adapter, int quantum) {
            super(context, bindingsCursor, adapter, quantum);
        }
    }
}
