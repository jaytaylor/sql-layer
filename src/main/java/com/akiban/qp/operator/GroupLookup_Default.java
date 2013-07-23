/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.qp.operator;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.HKeyRowType;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.explain.*;
import com.akiban.server.explain.std.LookUpOperatorExplainer;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**

 <h1>Overview</h1>

 GroupLookup_Default locates related group rows of both group rows and index rows.

 One expected usage is to locate the group row corresponding to an
 index row. For example, an index on customer.name yields index rows
 which GroupLookup_Default can then use to locate customer
 rows.

 Another expected usage is to locate ancestors higher in the group. For
 example, given either an item row or an item index row,
 GroupLookup_Default can be used to find the corresponding order and
 customer.

 Another expected usage is to locate descendants lower in the group. For
 example, given either an order group row,
 GroupLookup_Default can be used to find the corresponding items.

 <h1>Arguments</h1>

 <ul>

 <li><b>Group group:</b> The group containing the tables of interest.

 <li><b>RowType inputRowType:</b> Other tables will be located for input rows
 of this type.

 <li><b>Collection<UserTableRowType> outputRowTypes:</b> Tables to be located.

 <li><b>API.InputPreservationOption flag:</b> Indicates whether rows of type rowType
 will be preserved in the output stream (flag = KEEP_INPUT), or
 discarded (flag = DISCARD_INPUT).

 <li><b>int lookaheadQuantum:</b> Number of cursors to try to keep open by looking
  ahead in input stream, possibly across multiple outer loops.

 </ul>

 rowType may be an index row type or a group row type. For an index row
 type, rowType may be one of the outputRowTypes, and keepInput must be
 false.

 The group, inputRowType, and all outputRowTypes must belong to the same
 group.

 Each outputRowType must be an ancestor of the rowType or a descendant of it.

 <h1>Behavior</h1>

 For each input row, the hkey is obtained. For each ancestor type, the
 hkey is shortened if necessary, and the groupTable is then searched for
 a record with that exact hkey. For each descendant type, the hkey is lengthened
 with the ordinal of the shallowest descendant. All the retrieved records are written
 to the output stream in hkey order (ancestors before descendents), as
 is the input row if keepInput is true.

 <h1>Output</h1>

 Nothing else to say.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 For each input row, GroupLookup_Default does one random access for
 each ancestor type and one range access if there are any descendant types.

 <h1>Memory Requirements</h1>

 GroupLookup_Default stores in memory up to (number of ancestors +
 1) rows.

 */

class GroupLookup_Default extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s -> %s)", getClass().getSimpleName(), inputRowType, outputRowTypes());
    }

    // Operator interface

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
    }

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        if (lookaheadQuantum <= 1) {
            return new Execution(context, inputOperator.cursor(context, bindingsCursor));
        }
        else {
            return new LookaheadExecution(context, inputOperator.cursor(context, bindingsCursor), lookaheadQuantum);
        }
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

    // GroupLookup_Default interface

    public GroupLookup_Default(Operator inputOperator,
                               Group group,
                               RowType inputRowType,
                               Collection<UserTableRowType> outputRowTypes,
                               API.InputPreservationOption flag,
                               int lookaheadQuantum)
    {
        this.inputOperator = inputOperator;
        this.group = group;
        this.inputRowType = inputRowType;
        this.keepInput = flag == API.InputPreservationOption.KEEP_INPUT;
        this.lookaheadQuantum = lookaheadQuantum;

        ArgumentValidation.notEmpty("ancestorTypes", outputRowTypes);
        UserTableRowType tableRowType;
        if (inputRowType instanceof UserTableRowType) {
            tableRowType = (UserTableRowType)inputRowType;
        } else if (inputRowType instanceof IndexRowType) {
            // Keeping index rows not supported
            ArgumentValidation.isTrue("flag == API.InputPreservationOption.DISCARD_INPUT",
                                      flag == API.InputPreservationOption.DISCARD_INPUT);
            tableRowType = ((IndexRowType) inputRowType).tableType();
        } else if (inputRowType instanceof HKeyRowType) {
            ArgumentValidation.isTrue("flag == API.InputPreservationOption.DISCARD_INPUT",
                                      flag == API.InputPreservationOption.DISCARD_INPUT);
            tableRowType = ((Schema) inputRowType.schema()).userTableRowType(((HKeyRowType) inputRowType).hKey().userTable());
        } else {
            ArgumentValidation.isTrue("invalid rowType", false);
            tableRowType = null;
        }
        UserTable inputTable = tableRowType.userTable();
        this.ancestors = new ArrayList<>(outputRowTypes.size());
        List<UserTableRowType> branchOutputTypes = null;
        UserTable branchRoot = null;
        boolean outputInputTable = false;
        for (UserTableRowType outputRowType : outputRowTypes) {
            if (outputRowType == tableRowType) {
                ArgumentValidation.isTrue("flag == API.InputPreservationOption.DISCARD_INPUT",
                                          flag == API.InputPreservationOption.DISCARD_INPUT);
                outputInputTable = true;
            } else if (outputRowType.ancestorOf(tableRowType)) {
                ancestors.add(outputRowType.userTable());
            } else if (tableRowType.ancestorOf(outputRowType)) {
                if (branchOutputTypes == null)
                    branchOutputTypes = new ArrayList<>();
                branchOutputTypes.add(outputRowType);
                if (branchRoot != inputTable) {
                    // Get immediate child of input above desired output.
                    UserTable childTable = outputRowType.userTable();
                    while (true) {
                        UserTable parentTable = childTable.parentTable();
                        if (parentTable ==  inputTable) break;
                        childTable = parentTable;
                    }
                    if (branchRoot != childTable) {
                        if (branchRoot == null) {
                            branchRoot = childTable;
                        } else {
                            branchRoot = inputTable;
                        }
                    }
                }
            } else {
                // The old BranchLookup_Default would allow, say, item
                // to address, but the optimizer never generates that.
                ArgumentValidation.isTrue("ancestor or descendant", false);
            }
        }
        if (outputInputTable) {
            if (branchRoot != inputTable) {
                ancestors.add(inputTable);
            } else {
                branchOutputTypes.add(tableRowType);
            }
        }
        if (ancestors.size() > 1) {
            Collections.sort(ancestors, SORT_TABLE_BY_DEPTH);
        }
        if (branchOutputTypes == null) {
            this.branchOutputRowTypes = null;
            this.branchRootOrdinal = -1;
        } else {
            if (branchOutputTypes.size() > 1) {
                Collections.sort(branchOutputTypes, SORT_ROWTYPE_BY_DEPTH);
            }
            this.branchOutputRowTypes = branchOutputTypes;
            if (branchRoot == inputTable) {
                this.branchRootOrdinal = -1;
            } else {
                this.branchRootOrdinal = ordinal(branchRoot);
            }
        }
    }
    
    // For use by this class

    private static final Comparator<UserTable> SORT_TABLE_BY_DEPTH =
        new Comparator<UserTable>() 
        {
            @Override
            public int compare(UserTable x, UserTable y)
            {
                return x.getDepth() - y.getDepth();
            }
        };
    private static final Comparator<UserTableRowType> SORT_ROWTYPE_BY_DEPTH =
        new Comparator<UserTableRowType>() 
        {
            @Override
            public int compare(UserTableRowType x, UserTableRowType y)
            {
                return x.userTable().getDepth() - y.userTable().getDepth();
            }
        };

    private List<UserTableRowType> outputRowTypes() {
        List<UserTableRowType> types = new ArrayList<>();
        for (UserTable table : ancestors) {
            types.add(((Schema) inputRowType.schema()).userTableRowType(table));
        }
        if (branchOutputRowTypes != null) {
            types.addAll(branchOutputRowTypes);
        }
        return types;
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(GroupLookup_Default.class);
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: GroupLookup_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: GroupLookup_Default next");

    // Object state

    private final Operator inputOperator;
    private final Group group;
    private final RowType inputRowType;
    private final List<UserTable> ancestors;
    private final List<UserTableRowType> branchOutputRowTypes;
    private final boolean keepInput;
    private final int branchRootOrdinal;
    private final int lookaheadQuantum;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        for (UserTableRowType outputType : outputRowTypes()) {
            atts.put(Label.OUTPUT_TYPE, outputType.getExplainer(context));
        }
        return new LookUpOperatorExplainer(getName(), atts, inputRowType, keepInput, inputOperator, context);
    }

    // Inner classes

    private class Execution extends ChainedCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                input.open();
                advance();
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
                while (pending.isEmpty() && inputRow.isHolding()) {
                    advance();
                }
                Row row = pending.take();
                if (LOG_EXECUTION) {
                    LOG.debug("AncestorLookup: {}", row == null ? null : row);
                }
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
            if (input.isActive()) {
                input.close();
                ancestorRow.release();
                pending.clear();
            }
        }

        @Override
        public void destroy()
        {
            close();
            input.destroy();
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context, input);
            // Why + 1: Because the input row (whose ancestors get discovered) also goes into pending.
            this.pending = new PendingRows(ancestors.size() + 1);
            this.ancestorCursor = adapter().newGroupCursor(group);
        }

        // For use by this class

        private void advance()
        {
            Row currentRow = input.next();
            if (currentRow != null) {
                if (currentRow.rowType() == inputRowType) {
                    findAncestors(currentRow);
                }
                if (keepInput) {
                    pending.add(currentRow);
                }
                inputRow.hold(currentRow);
            } else {
                inputRow.release();
            }
        }

        private void findAncestors(Row inputRow)
        {
            assert pending.isEmpty();
            for (int i = 0; i < ancestors.size(); i++) {
                readAncestorRow(inputRow.ancestorHKey(ancestors.get(i)));
                if (ancestorRow.isHolding()) {
                    pending.add(ancestorRow.get());
                }
            }
        }

        private void readAncestorRow(HKey hKey)
        {
            try {
                ancestorCursor.rebind(hKey, false);
                ancestorCursor.open();
                Row retrievedRow = ancestorCursor.next();
                if (retrievedRow == null) {
                    ancestorRow.release();
                } else {
                    // Retrieved row might not actually be what we were looking for -- not all ancestors are present,
                    // (there are orphan rows).
                    ancestorRow.hold(hKey.equals(retrievedRow.hKey()) ? retrievedRow : null);
                }
            } finally {
                ancestorCursor.close();
            }
        }

        // Object state

        private final ShareHolder<Row> inputRow = new ShareHolder<>();
        private final GroupCursor ancestorCursor;
        private final ShareHolder<Row> ancestorRow = new ShareHolder<>();
        private final PendingRows pending;
    }

    private class LookaheadExecution extends OperatorCursor {
        // Cursor interface

        @Override
        public void open() {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                closed = false;
                cursorIndex = 0;
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
                Row outputRow = null;
                while (!closed && outputRow == null) {
                    // Get some more input rows, crossing bindings boundaries as
                    // necessary, and open cursors for them.
                    while (!bindingsExhausted && !inputRows[nextIndex].isHolding()) {
                        if (nextBindings == null) {
                            if (newBindings) {
                                nextBindings = currentBindings;
                                newBindings = false;
                            }
                            if (nextBindings == null) {
                                nextBindings = input.nextBindings();
                                if (nextBindings == null) {
                                    bindingsExhausted = true;
                                    break;
                                }
                                pendingBindings.add(nextBindings);
                            }
                            input.open();
                        }
                        Row row = input.next();
                        if (row == null) {
                            input.close();
                            nextBindings = null;
                        }
                        else {
                            inputRows[nextIndex].hold(row);
                            if (LOG_EXECUTION) {
                                LOG.debug("AncestorLookup: new input {}", row);
                            }
                            inputRowBindings[nextIndex] = nextBindings;
                            for (int i = 0; i < ncursors; i++) {
                                if (i == keepInputCursorIndex) continue;
                                int index = nextIndex * ncursors + i;
                                ancestorHKeys[index] = row.ancestorHKey(ancestors.get(i));
                                cursors[index].rebind(ancestorHKeys[index], false);
                                cursors[index].open();
                            }
                            nextIndex = (nextIndex + 1) % quantum;
                        }                        
                    }
                    // Now take ancestor rows from the front of those.
                    if (!inputRows[currentIndex].isHolding()) {
                        closed = true; // No more rows loaded.
                    }
                    else if (inputRowBindings[currentIndex] != currentBindings) {
                        closed = true; // Row came from another bindings.
                    }
                    else if (cursorIndex >= ncursors) {
                        // Done with this row.
                        inputRows[currentIndex].release();
                        inputRowBindings[currentIndex] = null;
                        currentIndex = (currentIndex + 1) % quantum;
                        cursorIndex = 0;
                    }
                    else if (cursorIndex == keepInputCursorIndex) {
                        outputRow = inputRows[currentIndex].get();
                        cursorIndex++;
                    }
                    else {
                        int index = currentIndex * ncursors + cursorIndex;
                        outputRow = cursors[index].next();
                        cursors[index].close();
                        if ((outputRow != null) && 
                            !ancestorHKeys[index].equals(outputRow.hKey())) {
                            // Not the row we wanted; no matching ancestor.
                            outputRow = null;
                        }
                        ancestorHKeys[index] = null;
                        cursorIndex++;
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("AncestorLookup: yield {}", outputRow);
                }
                return outputRow;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        @Override
        public void close() {
            CursorLifecycle.checkIdleOrActive(this);
            if (!closed) {
                // Any rows for the current bindings being closed need to be discarded.
                while (currentBindings == inputRowBindings[currentIndex]) {
                    inputRows[currentIndex].release();
                    inputRowBindings[currentIndex] = null;
                    for (int i = 0; i < ncursors; i++) {
                        if (i == keepInputCursorIndex) continue;
                        int index = currentIndex * ncursors + i;
                        cursors[index].close();
                        ancestorHKeys[index] = null;
                    }
                    currentIndex = (currentIndex + 1) % quantum;
                }
                closed = true;
            }
        }

        @Override
        public void destroy() {
            pendingBindings.clear();
            Arrays.fill(inputRowBindings, null);
            for (ShareHolder<Row> row : inputRows) {
                row.release();
            }
            for (GroupCursor ancestorCursor : cursors) {
                if (ancestorCursor != null) {
                    ancestorCursor.destroy();
                }
            }
            input.destroy();
        }

        @Override
        public boolean isIdle() {
            return !input.isDestroyed() && closed;
        }

        @Override
        public boolean isActive() {
            return !input.isDestroyed() && !closed;
        }

        @Override
        public boolean isDestroyed() {
            return input.isDestroyed();
        }

        @Override
        public void openBindings() {
            clearBindings();
            input.openBindings();
            currentIndex = nextIndex = 0;
            bindingsExhausted = false;
        }
                
        @Override
        public QueryBindings nextBindings() {
            CursorLifecycle.checkIdle(this);
            currentBindings = pendingBindings.poll();
            if (currentBindings == null) {
                currentBindings = input.nextBindings();
                if (currentBindings == null) {
                    bindingsExhausted = true;
                }
                newBindings = true; // Read from input, not pending, will need to open.
            }
            return currentBindings;
        }

        @Override
        public void closeBindings() {
            input.closeBindings();
            clearBindings();
        }

        @Override
        public void cancelBindings(QueryBindings bindings) {
            while (true) {
                QueryBindings pending = pendingBindings.peek();
                if (pending == null) break;
                if (!pending.isAncestor(bindings)) break;
                pendingBindings.remove();
            }
            while ((inputRowBindings[currentIndex] != null) &&
                   inputRowBindings[currentIndex].isAncestor(bindings)) {
                inputRows[currentIndex].release();
                inputRowBindings[currentIndex] = null;
                for (int i = 0; i < ncursors; i++) {
                    if (i == keepInputCursorIndex) continue;
                    int index = currentIndex * ncursors + i;
                    cursors[index].close();
                    ancestorHKeys[index] = null;
                }
                currentIndex = (currentIndex + 1) % quantum;
            }
            currentBindings = null;
            newBindings = false;
            input.cancelBindings(bindings);
            if ((nextBindings != null) && nextBindings.isAncestor(bindings)) {
                nextBindings = null;
            }
            closed = true;
        }

        // LookaheadExecution interface

        LookaheadExecution(QueryContext context, Cursor input, int quantum) {
            super(context);
            this.input = input;
            this.pendingBindings = new ArrayDeque<>(quantum+1);
            int nancestors = ancestors.size();
            if (keepInput) {
                this.keepInputCursorIndex = nancestors;
                this.ncursors = nancestors + 1;
            }
            else {
                this.keepInputCursorIndex = -1;
                this.ncursors = nancestors;
            }
            // Convert from number of cursors to number of input rows, rounding up.
            quantum = (quantum + nancestors - 1) / nancestors;
            this.quantum = quantum;
            this.inputRows = (ShareHolder<Row>[])new ShareHolder[quantum];
            this.inputRowBindings = new QueryBindings[quantum];
            for (int i = 0; i < this.inputRows.length; i++) {
                this.inputRows[i] = new ShareHolder<Row>();
            }
            this.cursors = new GroupCursor[quantum * ncursors];
            this.ancestorHKeys = new HKey[quantum * ncursors];
            for (int i = 0; i < this.cursors.length; i++) {
                if (i == keepInputCursorIndex) continue;
                this.cursors[i] = adapter().newGroupCursor(group);
            }
        }

        // For use by this class

        private void clearBindings() {
            if (nextBindings != null) {
                input.close();  // Starting over.
            }
            for (ShareHolder<Row> row : inputRows) {
                row.release();
            }
            pendingBindings.clear();
            Arrays.fill(inputRowBindings, null);
            currentBindings = nextBindings = null;
        }

        // Object state

        private final Cursor input;
        private final Queue<QueryBindings> pendingBindings;
        private final int quantum;
        private final ShareHolder<Row>[] inputRows;
        private final QueryBindings[] inputRowBindings;
        private final int ncursors, keepInputCursorIndex;
        private final GroupCursor[] cursors;
        private final HKey[] ancestorHKeys;
        private int currentIndex, nextIndex, cursorIndex;
        private QueryBindings currentBindings, nextBindings;
        private boolean bindingsExhausted, closed = true, newBindings;
    }
}
