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
import com.akiban.qp.rowtype.HKeyRowType;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.sql.optimizer.explain.*;
import com.akiban.sql.optimizer.explain.std.LookUpOperatorExplainer;
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
    protected Cursor cursor(QueryContext context)
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

    // AncestorLookup_Default interface

    public AncestorLookup_Default(Operator inputOperator,
                                  GroupTable groupTable,
                                  RowType rowType,
                                  Collection<UserTableRowType> ancestorTypes,
                                  API.InputPreservationOption flag)
    {
        validateArguments(rowType, ancestorTypes, flag);
        this.inputOperator = inputOperator;
        this.groupTable = groupTable;
        this.rowType = rowType;
        this.keepInput = flag == API.InputPreservationOption.KEEP_INPUT;
        // Sort ancestor types by depth
        this.ancestors = new ArrayList<UserTable>(ancestorTypes.size());
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
    private final GroupTable groupTable;
    private final RowType rowType;
    private final List<UserTable> ancestors;
    private final boolean keepInput;

    @Override
    public Explainer getExplainer(Map<Object, Explainer> extraInfo)
    {
        Attributes atts = new Attributes();
        if (extraInfo != null && extraInfo.containsKey(this))
            atts = ((OperationExplainer)extraInfo.get(this)).get();
        for (UserTable table : ancestors)
            atts.put(Label.ANCESTOR_TYPE, PrimitiveExplainer.getInstance(table.getName().toString()));
        
        return new LookUpOperatorExplainer("AncestorLookup_Default", atts, groupTable, rowType, keepInput, inputOperator, extraInfo);
    }

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
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
            TAP_NEXT.in();
            try {
                CursorLifecycle.checkIdleOrActive(this);
                checkQueryCancelation();
                while (pending.isEmpty() && inputRow.isHolding()) {
                    advance();
                }
                Row row = pending.take();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("AncestorLookup: {}", row == null ? null : row);
                }
                return row;
            } finally {
                TAP_NEXT.out();
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

        @Override
        public boolean isIdle()
        {
            return input.isIdle();
        }

        @Override
        public boolean isActive()
        {
            return input.isActive();
        }

        @Override
        public boolean isDestroyed()
        {
            return input.isDestroyed();
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context);
            this.input = input;
            // Why + 1: Because the input row (whose ancestors get discovered) also goes into pending.
            this.pending = new PendingRows(ancestors.size() + 1);
            this.ancestorCursor = adapter().newGroupCursor(groupTable);
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

        private final Cursor input;
        private final ShareHolder<Row> inputRow = new ShareHolder<Row>();
        private final GroupCursor ancestorCursor;
        private final ShareHolder<Row> ancestorRow = new ShareHolder<Row>();
        private final PendingRows pending;
    }
}
