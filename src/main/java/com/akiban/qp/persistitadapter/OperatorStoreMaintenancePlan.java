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

import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.physicaloperator.API;
import com.akiban.qp.physicaloperator.NoLimit;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.row.FlattenedRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.FlattenedRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

final class OperatorStoreMaintenancePlan {

    public PhysicalOperator rootOperator() {
        return rootOperator;
    }

    public RowType flattenedAncestorRowType() {
        return flattenedAncestorRowType;
    }

    public Row flattenLeft(Row row) {
        // validations, validations...
        assert (flattenedAncestorRowType==null) == (flatteningTypes==null)
                : flattenedAncestorRowType + ", " + flatteningTypes;
        if (flattenedAncestorRowType == null) {
            throw new IllegalStateException("no flattened row defined");
        }
        assert !flatteningTypes.isEmpty() : "flatteningTypes is empty";
        if (!row.rowType().equals(flattenedAncestorRowType)) {
            throw new IllegalArgumentException(String.format(
                    "row(%s) is of type %s; required %s", row, row.rowType(), flattenedAncestorRowType()
            ));
        }

        // finally :-)
        Row result = row;
        for (FlattenedRowType flattenedRowType : flatteningTypes) {
            result = new FlattenedRow(flattenedRowType, row, null, row.hKey());
        }
        return result;
    }

    public OperatorStoreMaintenancePlan(BranchTables branchTables,
                                        GroupIndex groupIndex,
                                        UserTableRowType rowType)
    {
        PlanCreationStruct struct = createGroupIndexMaintenancePlan(branchTables, groupIndex, rowType);
        this.rootOperator = struct.rootOperator;
        this.flattenedAncestorRowType = struct.flattenedParentRowType;
        this.flatteningTypes = struct.flatteningRowTypes;
    }

    private final PhysicalOperator rootOperator;
    private final RowType flattenedAncestorRowType;
    private final List<FlattenedRowType> flatteningTypes;

    // for use by unit tests
    static PlanCreationStruct createGroupIndexMaintenancePlan(
            Schema schema,
            GroupIndex groupIndex,
            UserTableRowType rowType)
    {
        return createGroupIndexMaintenancePlan(
                new BranchTables(schema, groupIndex),
                groupIndex,
                rowType
        );
    }

    // for use in this class

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

        PlanCreationStruct result = new PlanCreationStruct();

        boolean deep = !branchTables.leafMost().equals(rowType);
        PhysicalOperator plan = API.groupScan_Default(
                groupIndex.getGroup().getGroupTable(),
                NoLimit.instance(),
                com.akiban.qp.expression.API.variable(HKEY_BINDING_POSITION),
                deep
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
                    true
            );
        }

        RowType parentRowType = null;
        API.JoinType joinType = API.JoinType.RIGHT_JOIN;
        EnumSet<API.FlattenOption> options = EnumSet.noneOf(API.FlattenOption.class);
        for (RowType branchRowType : branchTables.fromRoot()) {
            if (parentRowType == null) {
                parentRowType = branchRowType;
            }
            else {
                // when we hit the left join of <previous stuff> to <the incoming row>, keep the <previous stuff>
                // row, and record its type.
                // For instance, in a COIH schema, with a GI on OIH:
                // * an incoming O should not keep/record anything
                // * an incoming I should keep/record CO left join rows
                // * an incoming H should keep/record COI left join rows
                if (branchRowType.equals(rowType)) {
                    result.flattenedParentRowType = parentRowType;
                    options.add(API.FlattenOption.KEEP_PARENT);
                    result.flatteningRowTypes = new ArrayList<FlattenedRowType>();
                }
                plan = API.flatten_HKeyOrdered(plan, parentRowType, branchRowType, joinType, options);
                parentRowType = plan.rowType();
                if (result.flatteningRowTypes != null) {
                    FlattenedRowType flattenedRowType = (FlattenedRowType) parentRowType;
                    result.flatteningRowTypes.add(flattenedRowType);
                }
                options.remove(API.FlattenOption.KEEP_PARENT);
            }
            if (branchRowType.equals(branchTables.rootMost())) {
                joinType = API.JoinType.LEFT_JOIN;
                options.add(API.FlattenOption.LEFT_JOIN_SHORTENS_HKEY);
            }
        }
        result.rootOperator = plan;
        return result;
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

    // package consts

    static final int HKEY_BINDING_POSITION = 0;

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

        public UserTableRowType leafMost() {
            return onlyBranch.get(onlyBranch.size()-1);
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
        public PhysicalOperator rootOperator;
        public RowType flattenedParentRowType;
        public List<FlattenedRowType> flatteningRowTypes;
    }
}
