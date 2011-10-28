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
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.expression.UnboundExpressions;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.ArrayBindings;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.ConstantColumnSelector;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.VariableExpression;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDataValueSource;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.conversion.Converters;

import java.util.*;

final class OperatorStoreMaintenance {

    public void run(OperatorStoreGIHandler.Action action, PersistitHKey hKey, RowData forRow, StoreAdapter adapter, OperatorStoreGIHandler handler) {
        Operator planOperator = rootOperator(action);
        if (planOperator == null)
            return;
        Bindings bindings = new ArrayBindings(1);
        final List<Column> lookupCols;
        UserTable userTable = rowType.userTable();
        if (usePKs(action)) {
            // use PKs
            lookupCols = userTable.getPrimaryKey().getColumns();
        }
        else {
            // use FKs
            lookupCols = new ArrayList<Column>();
            for (JoinColumn joinColumn : userTable.getParentJoin().getJoinColumns()) {
                lookupCols.add(joinColumn.getChild());
            }
        }

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
        cursor.open(bindings);
        try {
            Row row;
            while ((row = cursor.next()) != null) {
                if (row.rowType().equals(planOperator.rowType())) {
                    handler.handleRow(groupIndex, row, action);
                }
            }
        } finally {
            cursor.close();
        }
    }

    private Operator rootOperator(OperatorStoreGIHandler.Action action) {
        switch (action) {
        case STORE:
            return storePlan;
        case DELETE:
            return deletePlan;
        default: throw new AssertionError(action.name());
        }
    }

    private boolean usePKs(OperatorStoreGIHandler.Action action) {
        switch (action) {
        case STORE:
            return usePksStore;
        case DELETE:
            return usePksDelete;
        default: throw new AssertionError(action.name());
        }
    }

    public OperatorStoreMaintenance(BranchTables branchTables,
                                    GroupIndex groupIndex,
                                    UserTableRowType rowType)
    {
        PlanCreationStruct storePlan = createGroupIndexMaintenancePlan(branchTables, groupIndex, rowType, true);
        this.storePlan = storePlan.rootOperator;
        this.usePksStore = storePlan.usePKs;
        PlanCreationStruct deletePlan = createGroupIndexMaintenancePlan(branchTables, groupIndex, rowType, false);
        this.deletePlan = deletePlan.rootOperator;
        this.usePksDelete = deletePlan.usePKs;

        this.rowType = rowType;
        this.groupIndex = groupIndex;
    }

    private final Operator storePlan;
    private final Operator deletePlan;
    private final GroupIndex groupIndex;
    private final boolean usePksStore;
    private final boolean usePksDelete;
    private final UserTableRowType rowType;

    enum Relationship {
        PARENT, ID, CHILD
    }

    // for use in this class

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

        PlanCreationStruct result = new PlanCreationStruct();
        // compute if this is a no-op
        if (!forStoring && rowType.userTable().getDepth() == 0 && rowType != branchTables.rootMost()) {
            return result;
        }

        Schema schema = rowType.schema();

        final Relationship scanFrom;
        // this is a cleanup scan
        int index = branchTables.fromRootMost().indexOf(rowType);
        if (index < 0) {
            // incoming row is above the GI, so look downward
            scanFrom = Relationship.CHILD;
        }
        else if (groupIndex.getJoinType() == Index.JoinType.LEFT) {
            // incoming row is within the GI; look rootward, and search on the row's FK.
            int indexWithinGI = branchTables.fromRootMost().indexOf(rowType) - 1;
            scanFrom = indexWithinGI < 0
                    ? Relationship.ID
                    : Relationship.PARENT;
        }
        else if (groupIndex.getJoinType() == Index.JoinType.RIGHT) {
            int indexWithinGI = branchTables.fromRootMost().indexOf(rowType) + 1;
            scanFrom = indexWithinGI < branchTables.fromRootMost().size()
                    ? Relationship.CHILD
                    : Relationship.ID;
        }
        else {
            throw new AssertionError(groupIndex + " has join " + groupIndex.getJoinType());
        }


        final UserTableRowType startFromRowType;
        TableIndex startFromIndex;
        final boolean usePKs;

        switch (scanFrom) {
        case PARENT:
            usePKs = false;
            startFromRowType = schema.userTableRowType(rowType.userTable().parentTable());
            startFromIndex = startFromRowType.userTable().getPrimaryKey().getIndex();
            break;
        case ID:
            usePKs = true;
            startFromRowType = rowType;
            startFromIndex = startFromRowType.userTable().getPrimaryKey().getIndex();
            break;
        case CHILD:
            usePKs = true;
            int idIndex = branchTables.fromRoot().indexOf(rowType);
            startFromRowType = branchTables.fromRoot().get(idIndex+1);
            startFromIndex = fkIndex(startFromRowType.userTable(), rowType.userTable());
            break;
        default:
            throw new AssertionError(scanFrom.name());
        }

        result.usePKs = usePKs;
        UserTable startFrom = startFromRowType.userTable();
        List<Expression> pkExpressions = new ArrayList<Expression>(startFromIndex.getColumns().size());
        for (int i=0; i < startFrom.getPrimaryKey().getColumns().size(); ++i) {
            int fieldPos = i + 1; // position 0 is already claimed in OperatorStore
            Column col = startFrom.getPrimaryKey().getColumns().get(i);
            Expression pkField = new VariableExpression(col.getType().akType(), fieldPos);
            pkExpressions.add(pkField);
        }
        UnboundExpressions unboundExpressions = new RowBasedUnboundExpressions(startFromRowType, pkExpressions);
        IndexBound bound = new IndexBound(unboundExpressions, ConstantColumnSelector.ALL_ON);
        IndexKeyRange range = new IndexKeyRange(bound, true, bound, true);

        // the scan, main table and flattens

        Operator plan;
        IndexRowType pkRowType = schema.indexRowType(startFromIndex);
        plan = API.indexScan_Default(pkRowType, false, range);
        plan = API.branchLookup_Default(
                plan,
                startFrom.getGroup().getGroupTable(),
                pkRowType,
                branchTables.allTablesForBranch.get(0),
                API.LookupOption.DISCARD_INPUT
        );

        // RIGHT JOIN until the GI, and then the GI's join types

        RowType parentRowType = null;
        API.JoinType joinType = API.JoinType.RIGHT_JOIN;
        int branchStartDepth = branchTables.rootMost().userTable().getDepth() - 1;
        boolean withinBranch = branchStartDepth == -1;
        API.JoinType withinBranchJoin = operatorJoinType(groupIndex);
        for (UserTableRowType branchRowType : branchTables.fromRoot()) {
            if (parentRowType == null) {
                parentRowType = branchRowType;
            }
            else {
                plan = API.flatten_HKeyOrdered(plan, parentRowType, branchRowType, joinType);
                parentRowType = plan.rowType();
            }
            if (branchRowType.userTable().getDepth() == branchStartDepth) {
                withinBranch = true;
            } else if (withinBranch) {
                joinType = withinBranchJoin;
            }
        }
        result.rootOperator = plan;
        return result;
    }

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

    /**
     * Given a table with a FK column and a table with a PK column, gets the index from the FK table that
     * connects to the PK table.
     * @param fkTable the child table
     * @param pkTable the 
     * @return the fkTable's FK index
     */
    private static TableIndex fkIndex(UserTable fkTable, UserTable pkTable) {
        if (fkTable.getParentJoin() == null || fkTable.getParentJoin().getParent() != pkTable)
            throw new IllegalArgumentException(pkTable + " is not a parent of " + fkTable);
        List<Column> fkIndexCols = new ArrayList<Column>();
        for(JoinColumn joinColumn : fkTable.getParentJoin().getJoinColumns()) {
            fkIndexCols.add(joinColumn.getChild());
        }
        List<Column> candidateIndexColumns = new ArrayList<Column>();
        for (TableIndex index : fkTable.getIndexes()) {
            for (IndexColumn indexCol : index.getColumns()) {
                candidateIndexColumns.add(indexCol.getColumn());
            }
            if (candidateIndexColumns.equals(fkIndexCols)) {
                return index;
            }
            candidateIndexColumns.clear();
        }
        throw new AssertionError("no index found for columns: " + fkIndexCols);
    }

    // package consts

    private static final int HKEY_BINDING_POSITION = 0;

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
        public Operator rootOperator;
        public boolean usePKs;
    }
}
