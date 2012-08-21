/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.qp.operator;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.exec.Plannable;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.*;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.explain.*;
import com.akiban.server.explain.std.LookUpOperatorExplainer;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.Math.min;
import java.math.BigDecimal;

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
 be returned. The limit is specific to one UserTable. Deprecated
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
    public Cursor cursor(QueryContext context)
    {
        return new Execution(context, inputOperator.cursor(context));
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
                                UserTableRowType outputRowType,
                                API.InputPreservationOption flag,
                                Limit limit)
    {
        ArgumentValidation.notNull("inputRowType", inputRowType);
        ArgumentValidation.notNull("outputRowType", outputRowType);
        ArgumentValidation.notNull("limit", limit);
        ArgumentValidation.isTrue("outputRowType != inputRowType", outputRowType != inputRowType);
        ArgumentValidation.isTrue("inputRowType instanceof UserTableRowType || flag == API.InputPreservationOption.DISCARD_INPUT",
                                  inputRowType instanceof UserTableRowType || flag == API.InputPreservationOption.DISCARD_INPUT);
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
        this.keepInput = flag == API.InputPreservationOption.KEEP_INPUT;
        this.inputOperator = inputOperator;
        this.groupTable = groupTable;
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
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: BranchLookup_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: BranchLookup_Default next");

    // Object state

    private final Operator inputOperator;
    private final GroupTable groupTable;
    private final RowType inputRowType;
    private final UserTableRowType outputRowType;
    private final boolean keepInput;
    // If keepInput is true, inputPrecedesBranch controls whether input row appears before the retrieved branch.
    private final boolean inputPrecedesBranch;
    private final UserTable commonAncestor;
    private final int branchRootOrdinal;
    private final Limit limit;

    @Override
    public Explainer getExplainer(Map<Object, Explainer> extraInfo)
    {
        Attributes atts = new Attributes();
        if (extraInfo != null && extraInfo.containsKey(this))
            atts = ((OperationExplainer)extraInfo.get(this)).get();
        atts.put(Label.LIMIT, PrimitiveExplainer.getInstance(limit.toString()));
        atts.put(Label.OUTPUT_TYPE, PrimitiveExplainer.getInstance(outputRowType.userTable().getName().toString()));
        atts.put(Label.ANCESTOR_TYPE, PrimitiveExplainer.getInstance(commonAncestor.getName().toString()));
        
        return new LookUpOperatorExplainer("BranchLookup_Default", atts, groupTable, inputRowType, keepInput, inputOperator, extraInfo);
    }

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                inputCursor.open();
                advanceInput();
                idle = false;
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            TAP_NEXT.in();
            try {
                CursorLifecycle.checkIdleOrActive(this);
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
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close()
        {
            CursorLifecycle.checkIdleOrActive(this);
            if (!idle) {
                inputCursor.close();
                inputRow.release();
                lookupCursor.close();
                lookupRow.release();
                idle = true;
            }
        }

        @Override
        public void destroy()
        {
            close();
            inputCursor.destroy();
            lookupCursor.destroy();
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
        public boolean isDestroyed()
        {
            return inputCursor.isDestroyed();
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context);
            this.inputCursor = input;
            this.lookupCursor = adapter().newGroupCursor(groupTable);
            this.lookupRowHKey = adapter().newHKey(outputRowType.hKey());
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
                    computeLookupRowHKey(currentInputRow);
                    lookupCursor.rebind(lookupRowHKey, true);
                    lookupCursor.open();
                }
                inputRow.hold(currentInputRow);
            } else {
                inputRow.release();
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

        private final Cursor inputCursor;
        private final ShareHolder<Row> inputRow = new ShareHolder<Row>();
        private final GroupCursor lookupCursor;
        private final ShareHolder<Row> lookupRow = new ShareHolder<Row>();
        private final HKey lookupRowHKey;
        private LookupState lookupState;
        private boolean idle = true;
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
