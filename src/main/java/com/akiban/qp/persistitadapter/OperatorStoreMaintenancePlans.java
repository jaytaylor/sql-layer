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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.qp.util.SchemaCache;
import com.akiban.util.CachePair;

import java.util.*;

final class OperatorStoreMaintenancePlans {

    OperatorStoreMaintenance forRowType(GroupIndex groupIndex, UserTableRowType rowType) {
        Map<UserTableRowType,OperatorStoreMaintenance> typesToPlans = indexToTypesToOperators.get(groupIndex);
        if (typesToPlans == null) {
            throw new RuntimeException("no update plans found for group index " + groupIndex);
        }
        OperatorStoreMaintenance plan = typesToPlans.get(rowType);
        if (plan == null) {
            throw new RuntimeException("no plan found for row type " + rowType + " in GI " + groupIndex);
        }
        return plan;
    }

    public OperatorStoreMaintenancePlans(Schema schema, Collection<Group> groups) {
        indexToTypesToOperators = Collections.unmodifiableMap(generateGiPlans(schema, groups));
    }

    static OperatorStoreMaintenancePlans forAis(AkibanInformationSchema ais) {
        return CACHE_PER_AIS.get(ais);
    }

    /**
     * Create plan for the complete selection of all rows of the given GroupIndex (e.g. creating an
     * index on existing date).
     * @param schema Schema
     * @param groupIndex GroupIndex
     * @return PhysicalOperator
     */
    static Operator groupIndexCreationPlan(Schema schema, GroupIndex groupIndex) {
        OperatorStoreMaintenance.BranchTables branchTables = branchTablesRootToLeaf(schema, groupIndex);

        Operator plan = API.groupScan_Default(groupIndex.getGroup().getGroupTable());

        RowType parentRowType = null;
        API.JoinType joinType = API.JoinType.RIGHT_JOIN;
        EnumSet<API.FlattenOption> flattenOptions = EnumSet.noneOf(API.FlattenOption.class);
        final API.JoinType withinGIJoin;
        switch (groupIndex.getJoinType()) {
        case LEFT:
            withinGIJoin = API.JoinType.LEFT_JOIN;
            break;
        case RIGHT:
            withinGIJoin = API.JoinType.RIGHT_JOIN;
            break;
        default:
            throw new AssertionError(groupIndex.getJoinType().name());
        }
        for (RowType branchRowType : branchTables.fromRoot()) {
            if (parentRowType == null) {
                parentRowType = branchRowType;
            }
            else {
                plan = API.flatten_HKeyOrdered(plan, parentRowType, branchRowType, joinType, flattenOptions);
                parentRowType = plan.rowType();
            }
            if (branchRowType.equals(branchTables.rootMost())) {
                joinType = withinGIJoin;
            }
        }

        return plan;
    }

    // for use in this class

    private static OperatorStoreMaintenance.BranchTables branchTablesRootToLeaf(Schema schema, GroupIndex groupIndex) {
        return new OperatorStoreMaintenance.BranchTables(schema, groupIndex);
    }

    private static Map<GroupIndex, Map<UserTableRowType, OperatorStoreMaintenance>> generateGiPlans(
            Schema schema,
            Collection<Group> groups)
    {
        Map<GroupIndex,Map<UserTableRowType, OperatorStoreMaintenance>> giToMap
                 = new HashMap<GroupIndex, Map<UserTableRowType, OperatorStoreMaintenance>>();
        for (Group group : groups) {
            for (GroupIndex groupIndex : group.getIndexes()) {
                Map<UserTableRowType, OperatorStoreMaintenance> map = generateGIPlans(schema, groupIndex);
                giToMap.put(groupIndex, map);
            }
        }
        return Collections.unmodifiableMap(giToMap);
    }

    private static Map<UserTableRowType, OperatorStoreMaintenance> generateGIPlans(Schema schema, GroupIndex groupIndex) {
        OperatorStoreMaintenance.BranchTables branchTables = new OperatorStoreMaintenance.BranchTables(schema, groupIndex);
        Map<UserTableRowType, OperatorStoreMaintenance> plansPerType
                = new HashMap<UserTableRowType, OperatorStoreMaintenance>();
        for(UserTable table = groupIndex.leafMostTable(); table != null; table = table.parentTable()) {
            UserTableRowType rowType = schema.userTableRowType(table);
            OperatorStoreMaintenance plan = new OperatorStoreMaintenance(branchTables, groupIndex, rowType);
            plansPerType.put(rowType, plan);
        }
        return Collections.unmodifiableMap(plansPerType);
    }

    // object state

    private Map<GroupIndex,Map<UserTableRowType, OperatorStoreMaintenance>> indexToTypesToOperators;

    // consts

    private static final CachePair<AkibanInformationSchema, OperatorStoreMaintenancePlans> CACHE_PER_AIS
            = CachePair.using(
            new CachePair.CachedValueProvider<AkibanInformationSchema, OperatorStoreMaintenancePlans>() {
                @Override
                public OperatorStoreMaintenancePlans valueFor(AkibanInformationSchema ais) {
                    Schema schema = SchemaCache.globalSchema(ais);
                    return new OperatorStoreMaintenancePlans(schema, ais.getGroups().values());
                }
            }
    );
}
