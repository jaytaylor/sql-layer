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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.*;
import com.akiban.qp.operator.*;
import com.akiban.qp.row.FlattenedRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.FlattenedRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDataValueSource;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.PointTap;
import com.akiban.util.tap.Tap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

final class OperatorStoreMaintenance {

    public void run(OperatorStoreGIHandler.Action action, PersistitHKey hKey, RowData forRow, StoreAdapter adapter, OperatorStoreGIHandler handler) {
        if (storePlan.noMaintenanceRequired())
            return;
        Cursor cursor = null;
        boolean runTapEntered = false;
        ALL_TAP.in();
        try {
            Operator planOperator = rootOperator();
            if (planOperator == null)
                return;
            QueryContext context = new SimpleQueryContext(adapter);
            List<Column> lookupCols = rowType.userTable().getPrimaryKey().getColumns();

            context.setHKey(OperatorStoreMaintenance.HKEY_BINDING_POSITION, hKey);

            // Copy the values into the array bindings
            RowDataValueSource source = new RowDataValueSource();
            for (int i=0; i < lookupCols.size(); ++i) {
                int bindingsIndex = i+1;
                Column col = lookupCols.get(i);
                source.bind(col.getFieldDef(), forRow);
                context.setValue(bindingsIndex, source);
            }
            cursor = API.cursor(planOperator, context);
            RUN_TAP.in();
            runTapEntered = true;
            cursor.open();
            Row row;
            while ((row = cursor.next()) != null) {
                boolean actioned = false;
                if (row.rowType().equals(planOperator.rowType())) {
                    doAction(action, handler, row);
                    actioned = true;
                }
                else if (storePlan.incomingRowIsWithinGI) {
                    // "Natural" index cleanup. Look for the left half, but only if we need to
                    Index.JoinType giJoin = groupIndex.getJoinType();
                    switch (giJoin) {
                    case LEFT:
                        if (row.rowType().equals(storePlan.leftHalf) && useInvertType(action, context)) {
                            Row outerRow = new FlattenedRow(storePlan.topLevelFlattenType, row, null, row.hKey());
                            doAction(invert(action), handler, outerRow);
                            actioned = true;
                        }
                        break;
                    case RIGHT:
                        if (row.rowType().equals(storePlan.rightHalf) && useInvertType(action, context)) {
                            Row outerRow = new FlattenedRow(storePlan.topLevelFlattenType, null, row, row.hKey());
                            doAction(invert(action), handler, outerRow);
                            actioned = true;
                        }
                        break;
                    default: throw new AssertionError(giJoin.name());
                    }
                }
                else {
                    // Hkey cleanup. Look for the right half.
                    if (row.rowType().equals(storePlan.rightHalf)) {
                        Row outerRow = new FlattenedRow(storePlan.topLevelFlattenType, null, row, row.hKey());
                        doAction(invert(action), handler, outerRow);
                        actioned = true;

                    }
                }
                if (!actioned) {
                    extraTap(action).hit();
                }
            }
        } finally {
            if (cursor != null) {
                cursor.destroy();
            }
            if (runTapEntered) {
                RUN_TAP.out();
            }
            ALL_TAP.out();
        }
    }

    private boolean useInvertType(OperatorStoreGIHandler.Action action, QueryContext context) {
        switch (groupIndex.getJoinType()) {
        case LEFT:
            switch (action) {
            case STORE:
                return true;
            case DELETE:
                if (siblingsLookup == null)
                    return false;
                Cursor siblingsCounter = API.cursor(siblingsLookup, context);
                SIBLING_ALL_TAP.in();
                try {
                    siblingsCounter.open();
                    int siblings = 0;
                    while (siblingsCounter.next() != null) {
                        SIBLING_ROW_TAP.hit();
                        if (++siblings > 1)
                            return false;
                    }
                    return true;
                }
                finally {
                    siblingsCounter.destroy();
                    SIBLING_ALL_TAP.out();
                }
             default:
                 throw new AssertionError(action.name());
            }
        case RIGHT:
            return true;
        default: throw new AssertionError(groupIndex.getJoinType().name());
        }
    }

    private void doAction(OperatorStoreGIHandler.Action action, OperatorStoreGIHandler handler, Row row) {
        InOutTap actionTap = actionTap(action);
        actionTap.in();
        try {
            handler.handleRow(groupIndex, row, action);
        } finally {
            actionTap.out();
        }
    }

    private static OperatorStoreGIHandler.Action invert(OperatorStoreGIHandler.Action action) {
        switch (action) {
        case STORE:     return OperatorStoreGIHandler.Action.DELETE;
        case DELETE:    return OperatorStoreGIHandler.Action.STORE;
        default: throw new AssertionError(action.name());
        }
    }

    private Operator rootOperator() {
        return storePlan.rootOperator;
    }

    private InOutTap actionTap(OperatorStoreGIHandler.Action action) {
        if (action == null)
            return OTHER_TAP;
        switch (action) {
            case STORE:     return STORE_TAP;
            case DELETE:    return DELETE_TAP;
            default:        return OTHER_TAP;
        }
    }

    private PointTap extraTap(OperatorStoreGIHandler.Action action) {
        if (action == null)
            return EXTRA_OTHER_ROW_TAP;
        switch (action) {
            case STORE:     return EXTRA_STORE_ROW_TAP;
            case DELETE:    return EXTRA_DELETE_ROW_TAP;
            default:        return EXTRA_OTHER_ROW_TAP;
        }
    }

    public OperatorStoreMaintenance(BranchTables branchTables,
                                    GroupIndex groupIndex,
                                    UserTableRowType rowType)
    {
        this.storePlan = createGroupIndexMaintenancePlan(branchTables, groupIndex, rowType);
        siblingsLookup = createSiblingsFinder(groupIndex, branchTables, rowType);
        this.rowType = rowType;
        this.groupIndex = groupIndex;
    }

    private final PlanCreationStruct storePlan;
    private final Operator siblingsLookup;
    private final GroupIndex groupIndex;
    private final UserTableRowType rowType;

    // for use in this class

    private Operator createSiblingsFinder(GroupIndex groupIndex, BranchTables branchTables, UserTableRowType rowType) {
        // only bother doing this for tables *leafward* of the rootmost table in the GI
        if (rowType.userTable().getDepth() <= branchTables.rootMost().userTable().getDepth())
            return null;
        UserTable parentUserTable = rowType.userTable().parentTable();
        if (parentUserTable == null) {
            return null;
        }
        final GroupTable groupTable = groupIndex.getGroup().getGroupTable();
        final UserTableRowType parentRowType = branchTables.parentRowType(rowType);
        assert parentRowType != null;

        Operator plan = API.groupScan_Default(
                groupIndex.getGroup().getGroupTable(),
                HKEY_BINDING_POSITION,
                false,
                rowType.userTable(),
                branchTables.fromRoot().get(0).userTable()
        );
        plan = API.ancestorLookup_Default(plan, groupTable, rowType, Collections.singleton(parentRowType), API.InputPreservationOption.DISCARD_INPUT);
        plan = API.branchLookup_Default(plan, groupTable, parentRowType, rowType, API.InputPreservationOption.DISCARD_INPUT);
        plan = API.filter_Default(plan, Collections.singleton(rowType));
        return plan;
    }

    private static List<UserTableRowType> ancestors(RowType rowType, List<UserTableRowType> branchTables) {
        List<UserTableRowType> ancestors = new ArrayList<UserTableRowType>();
        for(UserTableRowType ancestor : branchTables) {
            if (ancestor.equals(rowType)) {
                return ancestors;
            }
            ancestors.add(ancestor);
        }
        throw new RuntimeException(rowType + "not found in " + branchTables);
    }

    private static PlanCreationStruct createGroupIndexMaintenancePlan(
            BranchTables branchTables,
            GroupIndex groupIndex,
            UserTableRowType rowType)
    {
        if (branchTables.isEmpty()) {
            throw new RuntimeException("group index has empty branch: " + groupIndex);
        }
        if (!branchTables.fromRoot().contains(rowType)) {
            throw new RuntimeException(rowType + " not in branch for " + groupIndex + ": " + branchTables);
        }

        PlanCreationStruct result = new PlanCreationStruct(rowType, groupIndex);

        Operator plan = API.groupScan_Default(
                groupIndex.getGroup().getGroupTable(),
                HKEY_BINDING_POSITION,
                false,
                rowType.userTable(),
                branchTables.fromRoot().get(0).userTable()
        );
        if (branchTables.fromRoot().size() == 1) {
            result.rootOperator = plan;
            return result;
        }
        if (!branchTables.leafMost().equals(rowType)) {
            // the incoming row isn't the leaf, so we have to get its ancestors along the branch
            UserTableRowType child = branchTables.childOf(rowType);
            plan = API.branchLookup_Default(
                    plan,
                    groupIndex.getGroup().getGroupTable(),
                    rowType,
                    child,
                    API.InputPreservationOption.KEEP_INPUT
            );
        }
        if (!branchTables.fromRoot().get(0).equals(rowType)) {
            plan = API.ancestorLookup_Default(
                    plan,
                    groupIndex.getGroup().getGroupTable(),
                    rowType,
                    ancestors(rowType, branchTables.fromRoot()),
                    API.InputPreservationOption.KEEP_INPUT
            );
        }

        // RIGHT JOIN until the GI, and then the GI's join types

        RowType parentRowType = null;
        API.JoinType joinType = API.JoinType.RIGHT_JOIN;
        int branchStartDepth = branchTables.rootMost().userTable().getDepth() - 1;
        boolean withinBranch = branchStartDepth == -1;
        API.JoinType withinBranchJoin = operatorJoinType(groupIndex);
        result.incomingRowIsWithinGI = rowType.userTable().getDepth() >= branchTables.rootMost().userTable().getDepth();

        for (UserTableRowType branchRowType : branchTables.fromRoot()) {
            boolean breakAtTop = result.incomingRowIsWithinGI && withinBranchJoin == API.JoinType.LEFT_JOIN;
            if (breakAtTop && branchRowType.equals(rowType)) {
                result.leftHalf = parentRowType;
                parentRowType = null;
            }
            if (parentRowType == null) {
                parentRowType = branchRowType;
            } else {
                plan = API.flatten_HKeyOrdered(plan, parentRowType, branchRowType, joinType);
                parentRowType = plan.rowType();
            }
            if (branchRowType.userTable().getDepth() == branchStartDepth) {
                withinBranch = true;
            } else if (withinBranch) {
                joinType = withinBranchJoin;
            }
            if ( (!breakAtTop) && branchRowType.equals(rowType)) {
                result.leftHalf = parentRowType;
                parentRowType = null;
            }
        }
        result.rightHalf = parentRowType;
        if (result.leftHalf != null && result.rightHalf != null) {
            API.JoinType topJoinType = rowType.userTable().getDepth() <= branchTables.rootMost().userTable().getDepth()
                    ? API.JoinType.RIGHT_JOIN
                    : joinType;
            plan = API.flatten_HKeyOrdered(plan, result.leftHalf, result.rightHalf, topJoinType, KEEP_BOTH);
            result.topLevelFlattenType = (FlattenedRowType) plan.rowType();
        }

        result.rootOperator = plan;
        return result;
    }

    private static final EnumSet<API.FlattenOption> KEEP_BOTH = EnumSet.of(
            API.FlattenOption.KEEP_PARENT,
            API.FlattenOption.KEEP_CHILD
    );

    private static API.JoinType operatorJoinType(Index index) {
        switch (index.getJoinType()) {
        case LEFT:
            return API.JoinType.LEFT_JOIN;
        case RIGHT:
            return API.JoinType.RIGHT_JOIN;
        default:
            throw new AssertionError(index.getJoinType().name());
        }
    }

    // package consts

    private static final int HKEY_BINDING_POSITION = 0;
    private static final InOutTap ALL_TAP = Tap.createTimer("GI maintenance: all");
    private static final InOutTap RUN_TAP = Tap.createTimer("GI maintenance: run");
    private static final InOutTap STORE_TAP = Tap.createTimer("GI maintenance: STORE");
    private static final InOutTap DELETE_TAP = Tap.createTimer("GI maintenance: DELETE");
    private static final InOutTap OTHER_TAP = Tap.createTimer("GI maintenance: OTHER");
    private static final InOutTap SIBLING_ALL_TAP = Tap.createTimer("GI maintenance: sibling all");
    private static final PointTap SIBLING_ROW_TAP = Tap.createCount("GI maintenance: sibling row");
    private static final PointTap EXTRA_STORE_ROW_TAP = Tap.createCount("GI maintenance: extra store");
    private static final PointTap EXTRA_DELETE_ROW_TAP = Tap.createCount("GI maintenance: extra delete");
    private static final PointTap EXTRA_OTHER_ROW_TAP = Tap.createCount("GI maintenance: extra other");

    // nested classes

    static class BranchTables {

        // BranchTables interface

        public List<UserTableRowType> fromRoot() {
            return allTablesForBranch;
        }

        public List<UserTableRowType> fromRootMost() {
            return onlyBranch;
        }

        public boolean isEmpty() {
            return fromRootMost().isEmpty();
        }

        public UserTableRowType rootMost() {
            return onlyBranch.get(0);
        }

        public UserTableRowType leafMost() {
            return onlyBranch.get(onlyBranch.size()-1);
        }

        public UserTableRowType childOf(UserTableRowType rowType) {
            int inputDepth = rowType.userTable().getDepth();
            int childDepth = inputDepth + 1;
            return allTablesForBranch.get(childDepth);
        }

        public UserTableRowType parentRowType(UserTableRowType rowType) {
            UserTableRowType parentType = null;
            for (UserTableRowType type : allTablesForBranch) {
                if (type.equals(rowType)) {
                    return parentType;
                }
                parentType = type;
            }
            throw new IllegalArgumentException(rowType + " not in branch: " + allTablesForBranch);
        }

        public BranchTables(Schema schema, GroupIndex groupIndex) {
            List<UserTableRowType> localTables = new ArrayList<UserTableRowType>();
            UserTable rootmost = groupIndex.rootMostTable();
            int branchRootmostIndex = -1;
            for (UserTable table = groupIndex.leafMostTable(); table != null; table = table.parentTable()) {
                if (table.equals(rootmost)) {
                    assert branchRootmostIndex == -1 : branchRootmostIndex;
                    branchRootmostIndex = table.getDepth();
                }
                localTables.add(schema.userTableRowType(table));
            }
            if (branchRootmostIndex < 0) {
                throw new RuntimeException("branch root not found! " + rootmost + " within " + localTables);
            }
            Collections.reverse(localTables);
            this.allTablesForBranch = Collections.unmodifiableList(localTables);
            this.onlyBranch = branchRootmostIndex == 0
                    ? allTablesForBranch
                    : allTablesForBranch.subList(branchRootmostIndex, allTablesForBranch.size());
        }

        // object state
        private final List<UserTableRowType> allTablesForBranch;
        private final List<UserTableRowType> onlyBranch;
    }

    static class PlanCreationStruct {

        public boolean noMaintenanceRequired() {
            return (!incomingRowIsWithinGI) && (incomingRowType.userTable().getDepth() == 0);
        }

        @Override
        public String toString() {
            return toString;
        }

        PlanCreationStruct(RowType forRow, GroupIndex forGi) {
            this.toString = String.format("for %s in %s", forRow, forGi.getIndexName().getName());
            this.incomingRowType = forRow;
        }

        public final String toString;
        public final RowType incomingRowType;

        public Operator rootOperator;
        public FlattenedRowType topLevelFlattenType;
        public RowType leftHalf;
        public RowType rightHalf;
        public boolean incomingRowIsWithinGI;


    }
}
