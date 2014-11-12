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

package com.foundationdb.server.store;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.row.FlattenedRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.FlattenedRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDataValueSource;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.PointTap;
import com.foundationdb.util.tap.Tap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

class StoreGIMaintenance {
    public void run(StoreGIHandler.Action action,
                    HKey hKey,
                    RowData forRow,
                    StoreAdapter adapter,
                    StoreGIHandler handler)
    {
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
            QueryBindings bindings = context.createBindings();
            List<Column> lookupCols = rowType.table().getPrimaryKeyIncludingInternal().getColumns();

            bindings.setHKey(StoreGIMaintenance.HKEY_BINDING_POSITION, hKey);

            // Copy the values into the array bindings
            RowDataValueSource pSource = new RowDataValueSource();
            for (int i=0; i < lookupCols.size(); ++i) {
                int bindingsIndex = i+1;
                Column col = lookupCols.get(i);
                pSource.bind(col.getFieldDef(), forRow);
                bindings.setValue(bindingsIndex, pSource);
            }
            cursor = API.cursor(planOperator, context, bindings);
            RUN_TAP.in();
            runTapEntered = true;
            cursor.openTopLevel();
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
                        if (row.rowType().equals(storePlan.leftHalf) && useInvertType(action, context, bindings) &&
                                !skipCascadeRow(action, row, handler)) {
                            Row outerRow = new FlattenedRow(storePlan.topLevelFlattenType, row, null, row.hKey());
                            doAction(invert(action), handler, outerRow);
                            actioned = true;
                        }
                        break;
                    case RIGHT:
                        if (row.rowType().equals(storePlan.rightHalf) && useInvertType(action, context, bindings) &&
                                !skipCascadeRow(action, row, handler)) {
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
                    if (row.rowType().equals(storePlan.rightHalf) && !skipCascadeRow(action, row, handler)) {
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
                cursor.closeTopLevel();
            }
            if (runTapEntered) {
                RUN_TAP.out();
            }
            ALL_TAP.out();
        }
    }

    private boolean skipCascadeRow(StoreGIHandler.Action action, Row row, StoreGIHandler handler) {
        return action == StoreGIHandler.Action.CASCADE &&
               row.rowType().typeComposition().tables().contains(handler.getSourceTable());
    }

    private boolean useInvertType(StoreGIHandler.Action action, QueryContext context, QueryBindings bindings) {
        switch (groupIndex.getJoinType()) {
        case LEFT:
            switch (action) {
            case CASCADE_STORE:
            case STORE:
                return true;
            case CASCADE:
            case DELETE:
                if (siblingsLookup == null)
                    return false;
                Cursor siblingsCounter = API.cursor(siblingsLookup, context, bindings);
                SIBLING_ALL_TAP.in();
                try {
                    siblingsCounter.openTopLevel();
                    int siblings = 0;
                    while (siblingsCounter.next() != null) {
                        SIBLING_ROW_TAP.hit();
                        if (++siblings > 1)
                            return false;
                    }
                    return true;
                }
                finally {
                    siblingsCounter.closeTopLevel();
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

    private void doAction(StoreGIHandler.Action action, StoreGIHandler handler, Row row)
    {
        InOutTap actionTap = actionTap(action);
        actionTap.in();
        try {
            handler.handleRow(groupIndex, row, action);
        } finally {
            actionTap.out();
        }
    }

    private static StoreGIHandler.Action invert(StoreGIHandler.Action action) {
        switch (action) {
        case STORE:     return StoreGIHandler.Action.DELETE;
        case DELETE:    return StoreGIHandler.Action.STORE;
        case CASCADE:   return StoreGIHandler.Action.CASCADE_STORE;
        case CASCADE_STORE:   return StoreGIHandler.Action.CASCADE_STORE;
        default: throw new AssertionError(action.name());
        }
    }

    private Operator rootOperator() {
        return storePlan.rootOperator;
    }

    private InOutTap actionTap(StoreGIHandler.Action action) {
        if (action == null)
            return OTHER_TAP;
        switch (action) {
            case STORE:     return STORE_TAP;
            case DELETE:    return DELETE_TAP;
            default:        return OTHER_TAP;
        }
    }

    private PointTap extraTap(StoreGIHandler.Action action) {
        if (action == null)
            return EXTRA_OTHER_ROW_TAP;
        switch (action) {
            case STORE:     return EXTRA_STORE_ROW_TAP;
            case DELETE:    return EXTRA_DELETE_ROW_TAP;
            default:        return EXTRA_OTHER_ROW_TAP;
        }
    }

    public StoreGIMaintenance(BranchTables branchTables,
                              GroupIndex groupIndex,
                              TableRowType rowType)
    {
        this.storePlan = createGroupIndexMaintenancePlan(branchTables, groupIndex, rowType);
        siblingsLookup = createSiblingsFinder(groupIndex, branchTables, rowType);
        this.rowType = rowType;
        this.groupIndex = groupIndex;
    }

    private final PlanCreationInfo storePlan;
    private final Operator siblingsLookup;
    private final GroupIndex groupIndex;
    private final TableRowType rowType;
    

    // for use in this class

    private Operator createSiblingsFinder(GroupIndex groupIndex, BranchTables branchTables, TableRowType rowType) {
        // only bother doing this for tables *leafward* of the rootmost table in the GI
        if (rowType.table().getDepth() <= branchTables.rootMost().table().getDepth())
            return null;
        Table parentTable = rowType.table().getParentTable();
        if (parentTable == null) {
            return null;
        }
        final Group group = groupIndex.getGroup();
        final TableRowType parentRowType = branchTables.parentRowType(rowType);
        assert parentRowType != null;

        Operator plan = API.groupScan_Default(
                groupIndex.getGroup(),
                HKEY_BINDING_POSITION,
                false,
                rowType.table(),
                branchTables.fromRoot().get(0).table()
        );
        plan = API.groupLookup_Default(plan, group, rowType, Collections.singleton(parentRowType), API.InputPreservationOption.DISCARD_INPUT, 1);
        plan = API.groupLookup_Default(plan, group, parentRowType, Collections.singleton(rowType), API.InputPreservationOption.DISCARD_INPUT, 1);
        return plan;
    }

    private static List<TableRowType> ancestors(RowType rowType, List<TableRowType> branchTables) {
        List<TableRowType> ancestors = new ArrayList<>();
        for(TableRowType ancestor : branchTables) {
            if (ancestor.equals(rowType)) {
                return ancestors;
            }
            ancestors.add(ancestor);
        }
        throw new RuntimeException(rowType + "not found in " + branchTables);
    }

    private static PlanCreationInfo createGroupIndexMaintenancePlan(BranchTables branchTables,
                                                                      GroupIndex groupIndex,
                                                                      TableRowType rowType)
    {
        if (branchTables.isEmpty()) {
            throw new RuntimeException("group index has empty branch: " + groupIndex);
        }
        if (!branchTables.fromRoot().contains(rowType)) {
            throw new RuntimeException(rowType + " not in branch for " + groupIndex + ": " + branchTables);
        }

        PlanCreationInfo result = new PlanCreationInfo(rowType, groupIndex);

        Operator plan = API.groupScan_Default(
                groupIndex.getGroup(),
                HKEY_BINDING_POSITION,
                false,
                rowType.table(),
                branchTables.fromRoot().get(0).table()
        );
        if (branchTables.fromRoot().size() == 1) {
            result.rootOperator = plan;
            return result;
        }
        if (!branchTables.leafMost().equals(rowType)) {
            // the incoming row isn't the leaf, so we have to get its ancestors along the branch
            List<TableRowType> children = branchTables.childrenOf(rowType);
            plan = API.groupLookup_Default(
                    plan,
                    groupIndex.getGroup(),
                    rowType,
                    children,
                    API.InputPreservationOption.KEEP_INPUT,
                    1
            );
        }
        if (!branchTables.fromRoot().get(0).equals(rowType)) {
            plan = API.groupLookup_Default(
                    plan,
                    groupIndex.getGroup(),
                    rowType,
                    ancestors(rowType, branchTables.fromRoot()),
                    API.InputPreservationOption.KEEP_INPUT,
                    1
            );
        }

        // RIGHT JOIN until the GI, and then the GI's join types

        RowType parentRowType = null;
        API.JoinType joinType = API.JoinType.RIGHT_JOIN;
        int branchStartDepth = branchTables.rootMost().table().getDepth() - 1;
        boolean withinBranch = branchStartDepth == -1;
        API.JoinType withinBranchJoin = operatorJoinType(groupIndex);
        result.incomingRowIsWithinGI = rowType.table().getDepth() >= branchTables.rootMost().table().getDepth();

        for (TableRowType branchRowType : branchTables.fromRoot()) {
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
            if (branchRowType.table().getDepth() == branchStartDepth) {
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
            API.JoinType topJoinType = rowType.table().getDepth() <= branchTables.rootMost().table().getDepth()
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

        public List<TableRowType> fromRoot() {
            return allTablesForBranch;
        }

        public List<TableRowType> fromRootMost() {
            return onlyBranch;
        }

        public boolean isEmpty() {
            return fromRootMost().isEmpty();
        }

        public TableRowType rootMost() {
            return onlyBranch.get(0);
        }

        public TableRowType leafMost() {
            return onlyBranch.get(onlyBranch.size()-1);
        }

        public List<TableRowType> childrenOf(TableRowType rowType) {
            int inputDepth = rowType.table().getDepth();
            int childDepth = inputDepth + 1;
            return allTablesForBranch.subList(childDepth, allTablesForBranch.size());
        }

        public TableRowType parentRowType(TableRowType rowType) {
            TableRowType parentType = null;
            for (TableRowType type : allTablesForBranch) {
                if (type.equals(rowType)) {
                    return parentType;
                }
                parentType = type;
            }
            throw new IllegalArgumentException(rowType + " not in branch: " + allTablesForBranch);
        }

        public BranchTables(Schema schema, GroupIndex groupIndex) {
            List<TableRowType> localTables = new ArrayList<>();
            Table rootmost = groupIndex.rootMostTable();
            int branchRootmostIndex = -1;
            for (Table table = groupIndex.leafMostTable(); table != null; table = table.getParentTable()) {
                if (table.equals(rootmost)) {
                    assert branchRootmostIndex == -1 : branchRootmostIndex;
                    branchRootmostIndex = table.getDepth();
                }
                localTables.add(schema.tableRowType(table));
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
        private final List<TableRowType> allTablesForBranch;
        private final List<TableRowType> onlyBranch;
    }

    static class PlanCreationInfo {

        public boolean noMaintenanceRequired() {
            return (!incomingRowIsWithinGI) && (incomingRowType.table().getDepth() == 0);
        }

        @Override
        public String toString() {
            return toString;
        }

        PlanCreationInfo(RowType forRow, GroupIndex forGi) {
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
