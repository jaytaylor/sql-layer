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

import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.physicaloperator.API;
import com.akiban.qp.physicaloperator.NoLimit;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

final class OperatorStoreMaintenancePlans {

    OperatorStoreMaintenancePlan forRowType(GroupIndex groupIndex, UserTableRowType rowType) {
        Map<UserTableRowType,OperatorStoreMaintenancePlan> typesToPlans = indexToTypesToOperators.get(groupIndex);
        if (typesToPlans == null) {
            throw new RuntimeException("no update plans found for group index " + groupIndex);
        }
        OperatorStoreMaintenancePlan plan = typesToPlans.get(rowType);
        if (plan == null) {
            throw new RuntimeException("no plan found for row type " + rowType + " in GI " + groupIndex);
        }
        return plan;
    }

    public OperatorStoreMaintenancePlans(Schema schema, Collection<Group> groups) {
        indexToTypesToOperators = Collections.unmodifiableMap(generateGiPlans(schema, groups));
    }

    /**
     * Create plan for the complete selection of all rows of the given GroupIndex (e.g. creating an
     * index on existing date).
     * @param schema Schema
     * @param groupIndex GroupIndex
     * @return PhysicalOperator
     */
    static PhysicalOperator groupIndexCreationPlan(Schema schema, GroupIndex groupIndex) {
        OperatorStoreMaintenancePlan.BranchTables branchTables = branchTablesRootToLeaf(schema, groupIndex);

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

        return plan;
    }

    // for use in this class

    private static OperatorStoreMaintenancePlan.BranchTables branchTablesRootToLeaf(Schema schema, GroupIndex groupIndex) {
        return new OperatorStoreMaintenancePlan.BranchTables(schema, groupIndex);
    }

    private static Map<GroupIndex, Map<UserTableRowType, OperatorStoreMaintenancePlan>> generateGiPlans(
            Schema schema,
            Collection<Group> groups)
    {
        Map<GroupIndex,Map<UserTableRowType, OperatorStoreMaintenancePlan>> giToMap
                 = new HashMap<GroupIndex, Map<UserTableRowType, OperatorStoreMaintenancePlan>>();
        for (Group group : groups) {
            for (GroupIndex groupIndex : group.getIndexes()) {
                Map<UserTableRowType, OperatorStoreMaintenancePlan> map = generateGIPlans(schema, groupIndex);
                giToMap.put(groupIndex, map);
            }
        }
        return Collections.unmodifiableMap(giToMap);
    }

    private static Map<UserTableRowType, OperatorStoreMaintenancePlan> generateGIPlans(Schema schema, GroupIndex groupIndex) {
        OperatorStoreMaintenancePlan.BranchTables branchTables = new OperatorStoreMaintenancePlan.BranchTables(schema, groupIndex);
        Map<UserTableRowType, OperatorStoreMaintenancePlan> plansPerType
                = new HashMap<UserTableRowType, OperatorStoreMaintenancePlan>();
        for(UserTable table = groupIndex.leafMostTable(); table != null; table = table.parentTable()) {
            UserTableRowType rowType = schema.userTableRowType(table);
            OperatorStoreMaintenancePlan plan = new OperatorStoreMaintenancePlan(branchTables, groupIndex, rowType);
            plansPerType.put(rowType, plan);
        }
        return Collections.unmodifiableMap(plansPerType);
    }

    // object state

    private Map<GroupIndex,Map<UserTableRowType, OperatorStoreMaintenancePlan>> indexToTypesToOperators;
}