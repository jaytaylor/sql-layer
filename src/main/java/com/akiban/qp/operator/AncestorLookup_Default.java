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

 AncestorLookup_Default locates ancestors of both group rows and index rows.

 One expected usage is to locate the group row corresponding to an
 index row. For example, an index on customer.name yields index rows
 which AncestorLookup_Default can then use to locate customer
 rows. (The ancestor relationship is reflexive, e.g. customer is
 considered to be an ancestor of customer.)

 Another expected usage is to locate ancestors higher in the group. For
 example, given either an item row or an item index row,
 AncestorLookup_Default can be used to find the corresponding order and
 customer.

 Unlike BranchLookup, AncestorLookup always locates 0-1 row per ancestor type.

 <h1>Arguments</h1>

 <ul>

 <li><b>GroupTable groupTable:</b> The group table containing the
 ancestors of interest.

 <li><b>RowType rowType:</b> Ancestors will be located for input rows
 of this type.

 <li><b>Collection<UserTableRowType> ancestorTypes:</b> Ancestor types to be located.

 <li><b>API.InputPreservationOption flag:</b> Indicates whether rows of type rowType
 will be preserved in the output stream (flag = KEEP_INPUT), or
 discarded (flag = DISCARD_INPUT).

 <li><b>int lookaheadQuantum:</b> Number of cursors to try to keep open by looking
  ahead in input stream, possibly across multiple bindings.

 </ul>

 rowType may be an index row type or a group row type. For a group row
 type, rowType must not be one of the ancestorTypes. For an index row
 type, rowType may be one of the ancestorTypes, and keepInput must be
 false (this may be relaxed in the future).

 The groupTable, rowType, and all ancestorTypes must belong to the same
 group.

 Each ancestorType must be an ancestor of the rowType (or, if rowType
 is an index type, then an ancestor of the index's table's type).

 <h1>Behavior</h1>

 For each input row, the hkey is obtained. For each ancestor type, the
 hkey is shortened if necessary, and the groupTable is then search for
 a record with that exact hkey. All the retrieved records are written
 to the output stream in hkey order (ancestors before descendents), as
 is the input row if keepInput is true.

 <h1>Output</h1>

 Nothing else to say.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 For each input row, AncestorLookup_Default does one random access for
 each ancestor type.

 <h1>Memory Requirements</h1>

 AncestorLookup_Default stores in memory up to (ancestorTypes.size() +
 1) rows.

 */

class AncestorLookup_Default extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s -> %s)", getClass().getSimpleName(), rowType, ancestors);
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

    // AncestorLookup_Default interface

    public AncestorLookup_Default(Operator inputOperator,
                                  Group group,
                                  RowType rowType,
                                  Collection<UserTableRowType> ancestorTypes,
                                  API.InputPreservationOption flag,
                                  int lookaheadQuantum)
    {
        validateArguments(rowType, ancestorTypes, flag);
        this.inputOperator = inputOperator;
        this.group = group;
        this.rowType = rowType;
        this.keepInput = flag == API.InputPreservationOption.KEEP_INPUT;
        this.lookaheadQuantum = lookaheadQuantum;
        // Sort ancestor types by depth
        this.ancestors = new ArrayList<>(ancestorTypes.size());
        for (UserTableRowType ancestorType : ancestorTypes) {
            this.ancestors.add(ancestorType.userTable());
        }
        if (this.ancestors.size() > 1) {
            Collections.sort(this.ancestors,
                             new Comparator<UserTable>()
                             {
                                 @Override
                                 public int compare(UserTable x, UserTable y)
                                 {
                                     return x.getDepth() - y.getDepth();
                                 }
                             });
        }
    }
    
    // For use by this class

    private void validateArguments(RowType rowType, Collection<UserTableRowType> ancestorTypes, API.InputPreservationOption flag)
    {
        ArgumentValidation.notEmpty("ancestorTypes", ancestorTypes);
        if (rowType instanceof IndexRowType) {
            // Keeping index rows not supported
            ArgumentValidation.isTrue("flag == API.InputPreservationOption.DISCARD_INPUT",
                                      flag == API.InputPreservationOption.DISCARD_INPUT);
            RowType tableRowType = ((IndexRowType) rowType).tableType();
            // Each ancestorType must be an ancestor of rowType. ancestorType = tableRowType is OK only if the input
            // is from an index. I.e., this operator can be used for an index lookup.
            for (UserTableRowType ancestorType : ancestorTypes) {
                ArgumentValidation.isTrue("ancestorType.ancestorOf(tableRowType)",
                                          ancestorType.ancestorOf(tableRowType));
                ArgumentValidation.isTrue("ancestorType.userTable().getGroup() == tableRowType.userTable().getGroup()",
                                          ancestorType.userTable().getGroup() == tableRowType.userTable().getGroup());
            }
        } else if (rowType instanceof UserTableRowType) {
            // Each ancestorType must be an ancestor of rowType. ancestorType = tableRowType is OK only if the input
            // is from an index. I.e., this operator can be used for an index lookup.
            for (RowType ancestorType : ancestorTypes) {
                ArgumentValidation.isTrue("ancestorType != tableRowType",
                                          ancestorType != rowType);
                ArgumentValidation.isTrue("ancestorType.ancestorOf(tableRowType)",
                                          ancestorType.ancestorOf(rowType));
                ArgumentValidation.isTrue("ancestorType.userTable().getGroup() == tableRowType.userTable().getGroup()",
                                          ancestorType.userTable().getGroup() == rowType.userTable().getGroup());
            }
        } else if (rowType instanceof HKeyRowType) {
            ArgumentValidation.isTrue("flag == API.InputPreservationOption.DISCARD_INPUT",
                                      flag == API.InputPreservationOption.DISCARD_INPUT);
            for (UserTableRowType ancestorType : ancestorTypes) {
                HKeyRowType hKeyRowType = (HKeyRowType) rowType;
                UserTableRowType tableRowType = ancestorType.schema().userTableRowType(hKeyRowType.hKey().userTable());
                ArgumentValidation.isTrue("ancestorType.ancestorOf(tableRowType)",
                                          ancestorType.ancestorOf(tableRowType));
                ArgumentValidation.isTrue("ancestorType.userTable().getGroup() == tableRowType.userTable().getGroup()",
                                          ancestorType.userTable().getGroup() == tableRowType.userTable().getGroup());
            }
        } else {
            ArgumentValidation.isTrue("invalid rowType", false);
        }
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(AncestorLookup_Default.class);
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: AncestorLookup_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: AncestorLookup_Default next");

    // Object state

    private final Operator inputOperator;
    private final Group group;
    private final RowType rowType;
    private final List<UserTable> ancestors;
    private final boolean keepInput;
    private final int lookaheadQuantum;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        for (UserTable table : ancestors) {
            atts.put(Label.OUTPUT_TYPE, ((Schema)rowType.schema()).userTableRowType(table).getExplainer(context));
        }
        return new LookUpOperatorExplainer(getName(), atts, rowType, keepInput, inputOperator, context);
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
                if (currentRow.rowType() == rowType) {
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
                ancestorIndex = -1;
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
                int nancestors = ancestors.size();
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
                            for (int i = 0; i < nancestors; i++) {
                                int index = nextIndex * nancestors + i;
                                ancestorHKeys[index] = row.ancestorHKey(ancestors.get(i));
                                ancestorCursors[index].rebind(ancestorHKeys[index], false);
                                ancestorCursors[index].open();
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
                    else if (ancestorIndex < 0) {
                        if (keepInput) {
                            outputRow = inputRows[currentIndex].get();
                        }
                        ancestorIndex++;
                    }
                    else if (ancestorIndex >= nancestors) {
                        // Done with this row.
                        inputRows[currentIndex].release();
                        inputRowBindings[currentIndex] = null;
                        currentIndex = (currentIndex + 1) % quantum;
                        ancestorIndex = -1;
                    }
                    else {
                        int index = currentIndex * nancestors + ancestorIndex;
                        outputRow = ancestorCursors[index].next();
                        ancestorCursors[index].close();
                        if ((outputRow != null) && 
                            !ancestorHKeys[index].equals(outputRow.hKey())) {
                            // Not the row we wanted; no matching ancestor.
                            outputRow = null;
                        }
                        ancestorHKeys[index] = null;
                        ancestorIndex++;
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
                int nancestors = ancestors.size();
                // Any rows for the current bindings being closed need to be discarded.
                while (currentBindings == inputRowBindings[currentIndex]) {
                    inputRows[currentIndex].release();
                    inputRowBindings[currentIndex] = null;
                    for (int i = 0; i < nancestors; i++) {
                        int index = currentIndex * nancestors + i;
                        ancestorCursors[index].close();
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
            for (GroupCursor ancestorCursor : ancestorCursors) {
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

        // LookaheadExecution interface

        LookaheadExecution(QueryContext context, Cursor input, int quantum) {
            super(context);
            this.input = input;
            this.pendingBindings = new ArrayDeque<>(quantum+1);
            int nancestors = ancestors.size();
            // Convert from number of cursors to number of input rows, rounding up.
            quantum = (quantum + nancestors - 1) / nancestors;
            this.quantum = quantum;
            this.inputRows = (ShareHolder<Row>[])new ShareHolder[quantum];
            this.inputRowBindings = new QueryBindings[quantum];
            for (int i = 0; i < this.inputRows.length; i++) {
                this.inputRows[i] = new ShareHolder<Row>();
            }
            this.ancestorCursors = new GroupCursor[quantum * nancestors];
            this.ancestorHKeys = new HKey[quantum * nancestors];
            for (int i = 0; i < this.ancestorCursors.length; i++) {
                this.ancestorCursors[i] = adapter().newGroupCursor(group);
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
        private final GroupCursor[] ancestorCursors;
        private final HKey[] ancestorHKeys;
        private int currentIndex, nextIndex, ancestorIndex;
        private QueryBindings currentBindings, nextBindings;
        private boolean bindingsExhausted, closed = true, newBindings;
    }
}
