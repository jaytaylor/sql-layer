/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.UserTableRowType;
import com.foundationdb.qp.util.SchemaCache;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

class StoreGIMaintenancePlans
{
    private static final Object CACHE_KEY = new Object();
    private static final CacheValueGenerator<StoreGIMaintenancePlans> CACHE_GENERATOR =
            new CacheValueGenerator<StoreGIMaintenancePlans>() {
        @Override
        public StoreGIMaintenancePlans valueFor(AkibanInformationSchema ais) {
            Schema schema = SchemaCache.globalSchema(ais);
            return new StoreGIMaintenancePlans(schema, ais.getGroups().values());
        }
    };


    private Map<GroupIndex,Map<UserTableRowType, StoreGIMaintenance>> indexToTypesToOperators;

    StoreGIMaintenance forRowType(GroupIndex groupIndex, UserTableRowType rowType) {
        Map<UserTableRowType,StoreGIMaintenance> typesToPlans = indexToTypesToOperators.get(groupIndex);
        if (typesToPlans == null) {
            throw new RuntimeException("no update plans found for group index " + groupIndex);
        }
        StoreGIMaintenance plan = typesToPlans.get(rowType);
        if (plan == null) {
            throw new RuntimeException("no plan found for row type " + rowType + " in GI " + groupIndex);
        }
        return plan;
    }

    public StoreGIMaintenancePlans(Schema schema, Collection<Group> groups) {
        indexToTypesToOperators = Collections.unmodifiableMap(generateGiPlans(schema, groups));
    }

    static StoreGIMaintenancePlans forAis(AkibanInformationSchema ais) {
        return ais.getCachedValue(CACHE_KEY, CACHE_GENERATOR);
    }

    /**
     * Create plan for the complete selection of all rows of the given GroupIndex (e.g. creating an
     * index on existing date).
     * @param schema Schema
     * @param groupIndex GroupIndex
     * @return PhysicalOperator
     */
    static Operator groupIndexCreationPlan(Schema schema, GroupIndex groupIndex) {
        StoreGIMaintenance.BranchTables branchTables = branchTablesRootToLeaf(schema, groupIndex);
        Operator plan = API.groupScan_Default(groupIndex.getGroup());
        RowType parentRowType = null;
        API.JoinType joinType = API.JoinType.RIGHT_JOIN;
        EnumSet<API.FlattenOption> flattenOptions = EnumSet.noneOf(API.FlattenOption.class);
        final API.JoinType withinGIJoin;
        switch(groupIndex.getJoinType()) {
            case LEFT:  withinGIJoin = API.JoinType.LEFT_JOIN;  break;
            case RIGHT: withinGIJoin = API.JoinType.RIGHT_JOIN; break;
            default:
                throw new AssertionError(groupIndex.getJoinType().name());
        }
        for(RowType branchRowType : branchTables.fromRoot()) {
            if(parentRowType == null) {
                parentRowType = branchRowType;
            } else {
                plan = API.flatten_HKeyOrdered(plan, parentRowType, branchRowType, joinType, flattenOptions);
                parentRowType = plan.rowType();
            }
            if(branchRowType.equals(branchTables.rootMost())) {
                joinType = withinGIJoin;
            }
        }
        return plan;
    }

    private static StoreGIMaintenance.BranchTables branchTablesRootToLeaf(Schema schema, GroupIndex groupIndex) {
        return new StoreGIMaintenance.BranchTables(schema, groupIndex);
    }

    private static Map<GroupIndex,Map<UserTableRowType,StoreGIMaintenance>> generateGiPlans(Schema schema,
                                                                                            Collection<Group> groups) {
        Map<GroupIndex,Map<UserTableRowType, StoreGIMaintenance>> giToMap = new HashMap<>();
        for(Group group : groups) {
            for(GroupIndex groupIndex : group.getIndexes()) {
                Map<UserTableRowType, StoreGIMaintenance> map = generateGIPlans(schema, groupIndex);
                giToMap.put(groupIndex, map);
            }
        }
        return Collections.unmodifiableMap(giToMap);
    }

    private static Map<UserTableRowType, StoreGIMaintenance> generateGIPlans(Schema schema, GroupIndex groupIndex) {
        StoreGIMaintenance.BranchTables branchTables = new StoreGIMaintenance.BranchTables(schema, groupIndex);
        Map<UserTableRowType, StoreGIMaintenance> plansPerType = new HashMap<>();
        for(UserTable table = groupIndex.leafMostTable(); table != null; table = table.parentTable()) {
            UserTableRowType rowType = schema.userTableRowType(table);
            StoreGIMaintenance plan = new StoreGIMaintenance(branchTables, groupIndex, rowType);
            plansPerType.put(rowType, plan);
        }
        return Collections.unmodifiableMap(plansPerType);
    }
}
