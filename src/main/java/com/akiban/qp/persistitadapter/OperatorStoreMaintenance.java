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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.ArrayBindings;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.row.FlattenedRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.FlattenedRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDataValueSource;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.util.Tap;

import java.util.*;

final class OperatorStoreMaintenance {

    public void run(OperatorStoreGIHandler.Action action, PersistitHKey hKey, RowData forRow, StoreAdapter adapter, OperatorStoreGIHandler handler) {
        if (groupIndex.getJoinType() == Index.JoinType.RIGHT)
            return; // TODO!!!
        ALL_TAP.in();
        Operator planOperator = rootOperator();
        if (planOperator == null)
            return;
        Bindings bindings = new ArrayBindings(1);
        final List<Column> lookupCols = rowType.userTable().getPrimaryKey().getColumns();

        bindings.set(OperatorStoreMaintenance.HKEY_BINDING_POSITION, hKey);

        // Copy the values into the array bindings
        ToObjectValueTarget target = new ToObjectValueTarget();
        RowDataValueSource source = new RowDataValueSource();
        for (int i=0; i < lookupCols.size(); ++i) {
            int bindingsIndex = i+1;
            Column col = lookupCols.get(i);
            source.bind((FieldDef)col.getFieldDef(), forRow);
            target.expectType(col.getType().akType());
            bindings.set(bindingsIndex, Converters.convert(source, target).lastConvertedValue());
        }

        Cursor cursor = API.cursor(planOperator, adapter);
        RUN_TAP.in();
        cursor.open(bindings);
        try {
            Row row;
            while ((row = cursor.next()) != null) {
                boolean actioned = false;
                if (row.rowType().equals(planOperator.rowType())) {
                    doAction(action, handler, row);
                    actioned = true;
                }
                else if (storePlan.incomingRowIsWithinGI) {
                    // "Natural" index cleanup. Look for the left half, but only if we need to
                    if (row.rowType().equals(storePlan.leftHalf) && useInvertType(action, bindings, adapter)) {
                        Row outerRow = new FlattenedRow(storePlan.topLevelFlattenType, row, null, row.hKey());
                        doAction(invert(action), handler, outerRow);
                        actioned = true;
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
            cursor.close();
            RUN_TAP.out();
        }
        ALL_TAP.out();
    }

    private boolean useInvertType(OperatorStoreGIHandler.Action action, Bindings bindings, StoreAdapter adapter) {
        switch (action) {
        case STORE:
            return true;
        case DELETE:
            if (siblingsLookup == null)
                return false;
            Cursor siblingsCounter = API.cursor(siblingsLookup, adapter);
            siblingsCounter.open(bindings);
            try {
                int siblings = 0;
                while (siblingsCounter.next() != null) {
                    if (++siblings > 1)
                        return false;
                }
                return true;
            }
            finally {
                siblingsCounter.close();
            }
         default:
             throw new AssertionError(action.name());
        }
    }

    private void doAction(OperatorStoreGIHandler.Action action, OperatorStoreGIHandler handler, Row row) {
        Tap.InOutTap actionTap = actionTap(action);
        actionTap.in();
        handler.handleRow(groupIndex, row, action);
        actionTap.out();
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

    private Tap.InOutTap actionTap(OperatorStoreGIHandler.Action action) {
        if (action == null)
            return OTHER_TAP;
        switch (action) {
            case STORE:     return STORE_TAP;
            case DELETE:    return DELETE_TAP;
            default:        return OTHER_TAP;
        }
    }

    private Tap.PointTap extraTap(OperatorStoreGIHandler.Action action) {
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
        this.storePlan = createGroupIndexMaintenancePlan(branchTables, groupIndex, rowType, true);
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
        // TODO this is a LJ GI rule!
        if (rowType.userTable().getDepth() <= branchTables.rootMost().userTable().getDepth())
            return null;
        UserTable parentUserTable = rowType.userTable().parentTable();
        if (parentUserTable == null) {
            return null;
        }
        final GroupTable groupTable = groupIndex.getGroup().getGroupTable();
        final RowType parentRowType = branchTables.parentRowType(rowType);
        assert parentRowType != null;

        Operator plan = API.groupScan_Default(
                groupIndex.getGroup().getGroupTable(),
                HKEY_BINDING_POSITION,
                false,
                rowType.userTable(),
                branchTables.fromRoot().get(0).userTable()
        );
        plan = API.ancestorLookup_Default(plan, groupTable, rowType, Collections.singleton(parentRowType), API.LookupOption.DISCARD_INPUT);
        plan = API.branchLookup_Default(plan, groupTable, parentRowType, rowType, API.LookupOption.DISCARD_INPUT);
        plan = API.filter_Default(plan, Collections.singleton(rowType));
        return plan;
    }

    private static List<RowType> ancestors(RowType rowType, List<? extends RowType> branchTables) {
        List<RowType> ancestors = new ArrayList<RowType>();
        for(RowType ancestor : branchTables) {
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
            UserTableRowType rowType,
            boolean forStoring)
    {
        if (branchTables.isEmpty()) {
            throw new RuntimeException("group index has empty branch: " + groupIndex);
        }
        if (!branchTables.fromRoot().contains(rowType)) {
            throw new RuntimeException(rowType + " not in branch for " + groupIndex + ": " + branchTables);
        }

        PlanCreationStruct result = new PlanCreationStruct(rowType, groupIndex);

        boolean deep = !branchTables.leafMost().equals(rowType);
        Operator plan = API.groupScan_Default(
                groupIndex.getGroup().getGroupTable(),
                HKEY_BINDING_POSITION,
                deep,
                rowType.userTable(),
                branchTables.fromRoot().get(0).userTable()
        );
        if (branchTables.fromRoot().size() == 1) {
            result.rootOperator = plan;
            return result;
        }
        if (!branchTables.fromRoot().get(0).equals(rowType)) {
            plan = API.ancestorLookup_Default(
                    plan,
                    groupIndex.getGroup().getGroupTable(),
                    rowType,
                    ancestors(rowType, branchTables.fromRoot()),
                    API.LookupOption.KEEP_INPUT
            );
        }

        // RIGHT JOIN until the GI, and then the GI's join types

        Schema schema = rowType.schema();
        RowType parentRowType = null;
        API.JoinType joinType = API.JoinType.RIGHT_JOIN;
        int branchStartDepth = branchTables.rootMost().userTable().getDepth() - 1;
        boolean withinBranch = branchStartDepth == -1;
        API.JoinType withinBranchJoin = operatorJoinType(groupIndex);
        result.incomingRowIsWithinGI = rowType.userTable().getDepth() >= branchTables.rootMost().userTable().getDepth();
        // TODO bookmark
        for (UserTableRowType branchRowType : branchTables.fromRoot()) {
            if (result.incomingRowIsWithinGI && branchRowType.equals(rowType)) {
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
            if ( (!result.incomingRowIsWithinGI) && branchRowType.equals(rowType)) {
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

    private static final EnumSet<API.FlattenOption> NO_FLATTEN_OPTIONS = EnumSet.noneOf(API.FlattenOption.class);
    private static final EnumSet<API.FlattenOption> KEEP_PARENT = EnumSet.of(API.FlattenOption.KEEP_PARENT);
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
    private static final Tap.InOutTap ALL_TAP = Tap.createTimer("GI maintenance: all");
    private static final Tap.InOutTap RUN_TAP = Tap.createTimer("GI maintenance: run");
    private static final Tap.InOutTap STORE_TAP = Tap.createTimer("GI maintenance: STORE");
    private static final Tap.InOutTap DELETE_TAP = Tap.createTimer("GI maintenance: DELETE");
    private static final Tap.InOutTap OTHER_TAP = Tap.createTimer("GI maintenance: OTHER");
    private static final Tap.PointTap EXTRA_STORE_ROW_TAP = Tap.createCount("GI maintenance: extra store");
    private static final Tap.PointTap EXTRA_DELETE_ROW_TAP = Tap.createCount("GI maintenance: extra delete");
    private static final Tap.PointTap EXTRA_OTHER_ROW_TAP = Tap.createCount("GI maintenance: extra other");

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

        @Override
        public String toString() {
            return toString;
        }

        PlanCreationStruct(RowType forRow, GroupIndex forGi) {
            this.toString = String.format("for %s in %s", forRow, forGi.getIndexName().getName());
        }

        public final String toString;

        public Operator rootOperator;
        public FlattenedRowType topLevelFlattenType;
        public RowType leftHalf;
        public RowType rightHalf;
        public boolean incomingRowIsWithinGI;
    }
}
