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
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.explain.std.LookUpOperatorExplainer;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import static java.lang.Math.min;

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

 <li><b>Collection<TableRowType> outputRowTypes:</b> Tables to be located.

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
                               Collection<TableRowType> outputRowTypes,
                               API.InputPreservationOption flag,
                               int lookaheadQuantum)
    {
        this.inputOperator = inputOperator;
        this.group = group;
        this.inputRowType = inputRowType;
        this.keepInput = flag == API.InputPreservationOption.KEEP_INPUT;
        this.lookaheadQuantum = lookaheadQuantum;

        ArgumentValidation.notEmpty("outputRowTypes", outputRowTypes);
        TableRowType tableRowType;
        if (inputRowType instanceof TableRowType) {
            tableRowType = (TableRowType)inputRowType;
        } else if (inputRowType instanceof IndexRowType) {
            // Keeping index rows not supported
            ArgumentValidation.isTrue("flag == API.InputPreservationOption.DISCARD_INPUT",
                                      flag == API.InputPreservationOption.DISCARD_INPUT);
            tableRowType = ((IndexRowType) inputRowType).tableType();
        } else if (inputRowType instanceof HKeyRowType) {
            ArgumentValidation.isTrue("flag == API.InputPreservationOption.DISCARD_INPUT",
                                      flag == API.InputPreservationOption.DISCARD_INPUT);
            tableRowType = ((Schema) inputRowType.schema()).tableRowType(((HKeyRowType) inputRowType).hKey().table());
        } else {
            ArgumentValidation.isTrue("invalid rowType", false);
            tableRowType = null;
        }
        Table inputTable = tableRowType.table();
        this.ancestors = new ArrayList<>(outputRowTypes.size());
        List<TableRowType> branchOutputRowTypes = null;
        Table branchRoot = null;
        boolean outputInputTable = false;
        for (TableRowType outputRowType : outputRowTypes) {
            if (outputRowType == tableRowType) {
                ArgumentValidation.isTrue("flag == API.InputPreservationOption.DISCARD_INPUT",
                                          flag == API.InputPreservationOption.DISCARD_INPUT);
                outputInputTable = true;
            } else if (outputRowType.ancestorOf(tableRowType)) {
                ancestors.add(outputRowType.table());
            } else if (tableRowType.ancestorOf(outputRowType)) {
                if (branchOutputRowTypes == null)
                    branchOutputRowTypes = new ArrayList<>();
                branchOutputRowTypes.add(outputRowType);
                if (branchRoot != inputTable) {
                    // Get immediate child of input above desired output.
                    Table childTable = outputRowType.table();
                    while (true) {
                        Table parentTable = childTable.getParentTable();
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
                branchOutputRowTypes.add(tableRowType);
            }
        }
        if (ancestors.size() > 1) {
            Collections.sort(ancestors, SORT_TABLE_BY_DEPTH);
        }
        if (branchOutputRowTypes == null) {
            this.branchOutputRowTypes = null;
            this.branchRootOrdinal = -1;
        } else {
            if (branchOutputRowTypes.size() > 1) {
                Collections.sort(branchOutputRowTypes, SORT_ROWTYPE_BY_DEPTH);
            }
            this.branchOutputRowTypes = branchOutputRowTypes;
            if (branchRoot == inputTable) {
                this.branchRootOrdinal = -1;
            } else {
                this.branchRootOrdinal = ordinal(branchRoot);
            }
        }
    }
    
    // For use by this class

    private static final Comparator<Table> SORT_TABLE_BY_DEPTH =
        new Comparator<Table>() 
        {
            @Override
            public int compare(Table x, Table y)
            {
                return x.getDepth() - y.getDepth();
            }
        };
    private static final Comparator<TableRowType> SORT_ROWTYPE_BY_DEPTH =
        new Comparator<TableRowType>()
        {
            @Override
            public int compare(TableRowType x, TableRowType y)
            {
                return x.table().getDepth() - y.table().getDepth();
            }
        };

    private List<TableRowType> outputRowTypes() {
        List<TableRowType> types = new ArrayList<>();
        for (Table table : ancestors) {
            types.add(((Schema) inputRowType.schema()).tableRowType(table));
        }
        if (branchOutputRowTypes != null) {
            types.addAll(branchOutputRowTypes);
        }
        return types;
    }

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

    private static final Logger LOG = LoggerFactory.getLogger(GroupLookup_Default.class);
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: GroupLookup_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: GroupLookup_Default next");
    
    // Object state

    private final Operator inputOperator;
    private final Group group;
    private final RowType inputRowType;
    private final List<Table> ancestors;
    private final List<TableRowType> branchOutputRowTypes;
    private final boolean keepInput;
    private final int branchRootOrdinal;
    private final int lookaheadQuantum;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        for (TableRowType outputType : outputRowTypes()) {
            atts.put(Label.OUTPUT_TYPE, outputType.getExplainer(context));
        }
        atts.put(Label.PIPELINE, PrimitiveExplainer.getInstance(lookaheadQuantum));
        return new LookUpOperatorExplainer(getName(), atts, inputRowType, keepInput, inputOperator, context);
    }

    // Inner classes

    private static enum LookupState
    {
        // Just opened or after any branch rows
        BETWEEN,
        // Ancestors filled in, any branch not open.
        ANCESTOR,
        // Scanning branch rows.
        BRANCH,
        // Input ran out.
        EXHAUSTED
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
                lookupState = LookupState.BETWEEN;
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
                while (pending.isEmpty() && (lookupState != LookupState.EXHAUSTED)) {
                    advance();
                }
                Row row = pending.poll();
                if (LOG_EXECUTION) {
                    LOG.debug("GroupLookup: yield {}", row);
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
            try {
                if (!lookupCursor.isClosed())
                    lookupCursor.close();
                lookupRow = null;
                pending.clear();
            } finally {
                super.close();
            }
        }
        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context, input);
            // Why + 1: Because the input row (whose ancestors get discovered) also goes into pending.
            this.pending = new ArrayDeque<>(ancestors.size() + 1);
            this.lookupCursor = adapter().newGroupCursor(group);
            if (branchOutputRowTypes != null) {
                this.lookupRowHKey = adapter().getKeyCreator().newHKey(inputRowType.hKey());
            }
            else {
                this.lookupRowHKey = null;
            }
        }

        // For use by this class

        private void advance()
        {
            switch (lookupState) {
            case BETWEEN:
                advanceInput();
                break;
            case ANCESTOR:
                advanceLookup();
                break;
            case BRANCH:
                advanceBranch();
                break;
            case EXHAUSTED:
            default:
                assert false: "GroupLookup_Default$Execution lookup state is bad: " + lookupState;
            }
        }

        private void advanceInput()
        {
            Row currentRow = input.next();
            if (currentRow != null) {
                if (currentRow.rowType() == inputRowType) {
                    findAncestors(currentRow);
                    lookupState = LookupState.ANCESTOR;
                }
                if (keepInput) {
                    pending.add(currentRow);
                }
                inputRow = currentRow;
            } else {
                inputRow = null;
                lookupState = LookupState.EXHAUSTED;
            }
        }

        private void findAncestors(Row inputRow)
        {
            assert pending.isEmpty();
            for (int i = 0; i < ancestors.size(); i++) {
                lookupRow = readAncestorRow(inputRow.ancestorHKey(ancestors.get(i)));
                if (lookupRow != null) {
                    pending.add(lookupRow);
                }
            }
        }

        private Row readAncestorRow(HKey hKey)
        {
            try {
                lookupCursor.rebind(hKey, false);
                lookupCursor.open();
                Row retrievedRow = lookupCursor.next();
                if (retrievedRow != null) {
                    // Retrieved row might not actually be what we were looking for -- not all ancestors are present,
                    // (there are orphan rows).
                    retrievedRow = hKey.equals(retrievedRow.hKey()) ? retrievedRow : null;
                }
                return retrievedRow;
            } finally {
                lookupCursor.close();
            }
        }

        private void advanceLookup()
        {
            if (branchOutputRowTypes == null) {
                lookupState = LookupState.BETWEEN;
                return;
            }
            lookupRow = null;
            computeBranchLookupRowHKey(inputRow);
            lookupCursor.rebind(lookupRowHKey, true);
            lookupCursor.open();
            lookupState = LookupState.BRANCH;
        }

        private void computeBranchLookupRowHKey(Row row)
        {
            HKey ancestorHKey = row.hKey(); // row.ancestorHKey(commonAncestor);
            ancestorHKey.copyTo(lookupRowHKey);
            if (branchRootOrdinal != -1) {
                lookupRowHKey.extendWithOrdinal(branchRootOrdinal);
            }
        }

        private void advanceBranch()
        {
            Row currentLookupRow = lookupCursor.next();
            lookupRow = null;
            if (currentLookupRow == null) {
                lookupState = LookupState.BETWEEN;
                lookupCursor.close();
            } else if (branchOutputRowTypes.contains(currentLookupRow.rowType())) {
                lookupRow = currentLookupRow;
            }
            if (lookupRow != null) {
                pending.add(lookupRow);
            }
        }

        // Object state

        private Row inputRow;
        private final GroupCursor lookupCursor;
        private Row lookupRow;
        private final Queue<Row> pending;
        private final HKey lookupRowHKey;
        private LookupState lookupState;
    }

    private class LookaheadExecution extends OperatorCursor {
        // Cursor interface

        @Override
        public void open() {
            TAP_OPEN.in();
            try {
                super.open();
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
                while (isActive() && outputRow == null) {
                    fillPipeline();
                    // Now take ancestor rows from the front of pipeline.
                    if (inputs[currentIndex].inputRow == null) {
                        setIdle(); // No more rows loaded.
                    }
                    else if (inputs[currentIndex].queryBindings != currentBindings) {
                        setIdle(); // Row came from another bindings.
                    }
                    else if (cursorIndex >= ncursors) {
                        // Done with this row.
                        inputs[currentIndex].inputRow = null;
                        inputs[currentIndex].queryBindings = null;
                        currentIndex = (currentIndex + 1) % quantum;
                        cursorIndex = 0;
                    }
                    else if (cursorIndex == keepInputCursorIndex) {
                        outputRow = inputs[currentIndex].inputRow; 
                        cursorIndex++;
                    }
                    else {
                        outputRow = inputs[currentIndex].cursors[cursorIndex].next();
                        if (cursorIndex == branchCursorIndex) {
                            // Get all matching rows from branch.
                            if (outputRow == null) {
                                inputs[currentIndex].cursors[cursorIndex].close();
                                cursorIndex++;
                            }
                            else if (!branchOutputRowTypes.contains(outputRow.rowType())) {
                                outputRow = null;
                            }
                        }
                        else {
                            inputs[currentIndex].cursors[cursorIndex].close();
                            if ((outputRow != null) && 
                                !inputs[currentIndex].lookupHKeys[cursorIndex].equals(outputRow.hKey())) {
                                // Not the row we wanted; no matching ancestor.
                                outputRow = null;
                            }
                            inputs[currentIndex].lookupHKeys[cursorIndex] = null;
                            cursorIndex++;
                        }
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("GroupLookup: yield {}", outputRow);
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
            try {
                // Any rows for the current bindings being closed need to be discarded.
                while (currentBindings == inputs[currentIndex].queryBindings) {
                    inputs[currentIndex].clearState();
                    currentIndex = (currentIndex + 1) % quantum;
                }
                if (inputs[nextIndex].inputRow == null && !input.isClosed()) {
                    input.close();
                    nextBindings = null;
                }
            } finally {
                super.close();
            }
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
            CursorLifecycle.checkClosed(this);
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
            CursorLifecycle.checkClosed(this);
            while (true) {
                QueryBindings pending = pendingBindings.peek();
                if (pending == null) break;
                if (!pending.isAncestor(bindings)) break;
                pendingBindings.remove();
            }
            
            while ((inputs[currentIndex].queryBindings != null) &&
                        inputs[currentIndex].queryBindings.isAncestor(bindings)) {
                inputs[currentIndex].clearState();
                currentIndex = (currentIndex + 1) % quantum;
            }
            currentBindings = null;
            newBindings = false;
            input.cancelBindings(bindings);
            if ((nextBindings != null) && nextBindings.isAncestor(bindings)) {
                nextBindings = null;
            }
        }

        // LookaheadExecution interface

        LookaheadExecution(QueryContext context, Cursor input, int quantum) {
            super(context);
            this.input = input;
            this.pendingBindings = new ArrayDeque<>(quantum+1);
            int nancestors = ancestors.size();
            int ncursors = nancestors; // Number of actual cursors (for quantum).
            int nindex = ncursors; // Number of slots.
            if (keepInput) {
                this.keepInputCursorIndex = nindex++;
            }
            else {
                this.keepInputCursorIndex = -1;
            }
            if (branchOutputRowTypes != null) {
                this.branchCursorIndex = nindex++;
                ncursors++;
            }
            else {
                this.branchCursorIndex = -1;
            }
            // Convert from number of cursors to number of input rows, rounding up.
            quantum = (quantum + ncursors - 1) / ncursors;
            this.quantum = quantum;
            this.ncursors = nindex;
            this.inputs = new InputState[quantum];
            for (int j = 0; j < quantum; j++) {
                this.inputs[j] = new InputState (nindex, keepInputCursorIndex, branchCursorIndex);
            }
        }

        // For use by this class

        private void clearBindings() {
            CursorLifecycle.checkClosed(this);
            for (InputState in : inputs) {
                in.inputRow = null;
                in.queryBindings = null;
            }
            pendingBindings.clear();
            currentBindings = nextBindings = null;
        }
        
        private void fillPipeline() {
            // Get some more input rows, crossing bindings boundaries as
            // necessary, and open cursors for them.
            while (!bindingsExhausted && inputs[nextIndex].inputRow == null) {
                if (nextBindings == null) {
                    if (newBindings) {
                        nextBindings = currentBindings;
                        newBindings = false;
                    }
                    while ((nextBindings == null) ||
                           (nextBindings.getDepth() != currentBindings.getDepth())) {
                        nextBindings = input.nextBindings();
                        if (nextBindings == null) {
                            bindingsExhausted = true;
                            return;
                        }
                        pendingBindings.add(nextBindings);
                    }
                    input.open();
                }
                Row row = input.next();
                if (row == null) {
                    // This is correct, close the input to allow
                    // nextBindings() to process correctly.
                    input.close();
                    nextBindings = null;
                }
                else {
                    InputState inputState = inputs[nextIndex];
                    inputState.inputRow = row;
                    if (LOG_EXECUTION) {
                        LOG.debug("GroupLookup: new input {}", row);
                    }
                    inputState.queryBindings = nextBindings;
                    for (int i = 0; i < ncursors; i++) {
                        if (i == keepInputCursorIndex) continue;
                        boolean deep = false;
                        if (i == branchCursorIndex) {
                            row.hKey().copyTo(inputState.lookupHKeys[i]);
                            if (branchRootOrdinal != -1) {
                                inputState.lookupHKeys[i].extendWithOrdinal(branchRootOrdinal);
                            }
                            deep = true;
                        }
                        else {
                            inputState.lookupHKeys[i] = row.ancestorHKey(ancestors.get(i));
                        }
                        inputState.cursors[i].rebind(inputState.lookupHKeys[i], deep);
                        inputState.cursors[i].open();
                    }
                    nextIndex = (nextIndex + 1) % quantum;
                }
            }
        }

        private class InputState 
        {
            public Row inputRow;
            public QueryBindings queryBindings;
            public final GroupCursor[] cursors;
            public final HKey[] lookupHKeys;
            //public final GroupCursor keepIndexCursor;
            
            public InputState (int slots, int keepInputCursorIndex, int branchCursorIndex) {
                inputRow = null;
                queryBindings = null;
                cursors = new GroupCursor[slots];
                lookupHKeys = new HKey[slots];
                for (int i = 0; i < slots; i++) {
                    if (i != keepInputCursorIndex)
                        this.cursors[i] = adapter().newGroupCursor(group);
                    if (i == branchCursorIndex)
                        this.lookupHKeys[i] = adapter().getKeyCreator().newHKey(inputRowType.hKey());
                }
            }
            
            public void clearState() {
                inputRow = null;
                queryBindings = null;
                for (int i = 0; i < cursors.length; i++) {
                    if (i == keepInputCursorIndex) continue;
                    if (!cursors[i].isClosed()) { cursors[i].close();}
                    lookupHKeys[i] = null;
                }
            }
        }
        
        // Object state

        private final Cursor input;
        private final Queue<QueryBindings> pendingBindings;
        private final int quantum;
        private final InputState[] inputs;
        
        
        //private final Row[] inputRows;
        //private final QueryBindings[] inputRowBindings;
        //private final GroupCursor[][] cursors;
        //private final HKey[][] lookupHKeys;
        private final int ncursors, keepInputCursorIndex, branchCursorIndex;
        private int currentIndex, nextIndex, cursorIndex;
        private QueryBindings currentBindings, nextBindings;
        private boolean bindingsExhausted, newBindings;
    }
}
