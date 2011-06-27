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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.physicaloperator.API;
import com.akiban.qp.physicaloperator.NoLimit;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.qp.util.SchemaCache;
import com.akiban.util.CachePair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.akiban.qp.physicaloperator.API.FlattenOption;

class MaintenancePlanCreator
        implements CachePair.CachedValueProvider<AkibanInformationSchema, Map<GroupIndex, Map<UserTableRowType,MaintenancePlan>>>
{

    // CachedValueProvider interface

    @Override
    public Map<GroupIndex, Map<UserTableRowType, MaintenancePlan>> valueFor(AkibanInformationSchema ais) {
        Schema schema = SchemaCache.globalSchema(ais);
        Map<GroupIndex, Map<UserTableRowType, MaintenancePlan>> giToMapMap
                = new HashMap<GroupIndex, Map<UserTableRowType, MaintenancePlan>>();
        for (Group group : ais.getGroups().values()) {
            for (GroupIndex groupIndex : group.getIndexes()) {
                Map<UserTableRowType, MaintenancePlan> plansPerType = generateGiPlans(schema, groupIndex);
                giToMapMap.put(groupIndex, plansPerType);
            }
        }
        return Collections.unmodifiableMap(giToMapMap);
    }

    // for use by this package (in production)

    /**
     * Create plan for the complete selection of all rows of the given GroupIndex (e.g. creating an
     * index on existing date).
     * @param schema Schema
     * @param groupIndex GroupIndex
     * @return PhysicalOperator
     */
    static MaintenancePlan groupIndexCreationPlan(Schema schema, GroupIndex groupIndex) {
        BranchTables branchTables = branchTablesRootToLeaf(schema, groupIndex);

        PhysicalOperator plan = API.groupScan_Default(groupIndex.getGroup().getGroupTable(), NoLimit.instance());

        RowType parentRowType = null;
        API.JoinType joinType = API.JoinType.RIGHT_JOIN;
        for (RowType branchRowType : branchTables.fromRoot()) {
            if (parentRowType == null) {
                parentRowType = branchRowType;
            }
            else {
                plan = API.flatten_HKeyOrdered(plan, parentRowType, branchRowType, joinType);
                parentRowType = plan.rowType();
            }
            if (branchRowType.equals(branchTables.rootMost())) {
                joinType = API.JoinType.INNER_JOIN;
            }
        }

        return new MaintenancePlan(plan, null);
    }

    // for use by unit tests
    
    static MaintenancePlan createGroupIndexMaintenancePlan(Schema schema, GroupIndex groupIndex,
                                                            UserTableRowType rowType) {
        BranchTables branchTables = branchTablesRootToLeaf(schema, groupIndex);
        if (branchTables.isEmpty()) {
            throw new RuntimeException("group index has empty branch: " + groupIndex);
        }
        if (!branchTables.fromRoot().contains(rowType)) {
            throw new RuntimeException(rowType + " not in branch for " + groupIndex + ": " + branchTables);
        }

        boolean deep = !branchTables.leafMost().equals(rowType);
        PhysicalOperator plan = API.groupScan_Default(
                groupIndex.getGroup().getGroupTable(),
                NoLimit.instance(),
                com.akiban.qp.expression.API.variable(HKEY_BINDING_POSITION),
                deep
        );
        if (branchTables.fromRoot().size() == 1) {
            assert !deep : "deep scan although GI branch was size 1: " + groupIndex;
            return new MaintenancePlan(plan, null); // there is no ancestor row
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

        EnumSet<FlattenOption> flattenOptions = RIGHT_JOIN_OPTIONS;
        API.JoinType joinType = API.JoinType.RIGHT_JOIN;
        RowType flattenedParentRowType = null;
        for (RowType branchRowType : branchTables.fromRoot()) {
            if (parentRowType == null) {
                parentRowType = branchRowType;
            }
            else {
                if (branchRowType.equals(rowType)) {
                    assert flattenedParentRowType == null : flattenedParentRowType;
                    flattenedParentRowType = parentRowType;
                }
                plan = API.flatten_HKeyOrdered(plan, parentRowType, branchRowType, joinType, flattenOptions);
                parentRowType = plan.rowType();

            }
            if (branchRowType.equals(branchTables.rootMost())) {
                joinType = API.JoinType.LEFT_JOIN;
                flattenOptions = LEFT_JOIN_OPTIONS;
            }
        }
        return new MaintenancePlan(plan, flattenedParentRowType);
    }

    // static helpers for use in this class

    private static BranchTables branchTablesRootToLeaf(Schema schema, GroupIndex groupIndex) {
        return new BranchTables(schema, groupIndex);
    }

    private static Map<UserTableRowType, MaintenancePlan> generateGiPlans(Schema schema, GroupIndex groupIndex) {
        Map<UserTableRowType, MaintenancePlan> plansPerType = new HashMap<UserTableRowType, MaintenancePlan>();
        for(UserTable table = groupIndex.leafMostTable(); table != null; table = table.parentTable()) {
            UserTableRowType rowType = schema.userTableRowType(table);
            MaintenancePlan plan = createGroupIndexMaintenancePlan(schema, groupIndex, rowType);
            plansPerType.put(rowType, plan);
        }
        return Collections.unmodifiableMap(plansPerType);
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
    private static final EnumSet<FlattenOption> RIGHT_JOIN_OPTIONS = EnumSet.noneOf(FlattenOption.class);
    private static final EnumSet<FlattenOption> LEFT_JOIN_OPTIONS = EnumSet.of(FlattenOption.LEFT_JOIN_SHORTENS_HKEY);

    // nested classes

    private static class BranchTables {

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
}
