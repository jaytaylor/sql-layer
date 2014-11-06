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

package com.foundationdb.sql.optimizer.rule.cost;

import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.sql.optimizer.OptimizerTestBase;
import com.foundationdb.sql.optimizer.plan.*;
import com.foundationdb.sql.optimizer.plan.TableGroupJoinTree.TableGroupJoinNode;
import com.foundationdb.sql.optimizer.rule.RulesTestHelper;

import com.foundationdb.ais.model.*;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MString;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.util.*;

public class CostEstimatorTest
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "costing");
    public static final String SCHEMA = OptimizerTestBase.DEFAULT_SCHEMA;

    protected AkibanInformationSchema ais;
    protected TableTree tree;
    protected CostEstimator costEstimator;

    @Before
    public void loadSchema() throws Exception {
        ais = OptimizerTestBase.parseSchema(new File(RESOURCE_DIR, "schema.ddl"));
        RulesTestHelper.ensureRowDefs(ais);
        tree = new TableTree();
        costEstimator = new TestCostEstimator(ais, new Schema(ais), new File(RESOURCE_DIR, "stats.yaml"), false, new Properties());
    }

    protected Table table(String name) {
        return ais.getTable(SCHEMA, name);
    }

    protected Index index(String table, String name) {
        return table(table).getIndex(name);
    }

    protected Index groupIndex(String name) {
        for (Group group : ais.getGroups().values()) {
            Index index = group.getIndex(name);
            if (index != null) {
                return index;
            }
        }
        return null;
    }

    protected TableNode tableNode(String name) {
        return tree.addNode(table(name));
    }

    protected TableSource tableSource(String name) {
        return new TableSource(tableNode(name), true, name);
    }

    protected static ExpressionNode constant(Object value, TInstance type) {
        return new ConstantExpression (value, type);
    }

    protected static ExpressionNode variable(TInstance type) {
        return new ParameterExpression (0, type.dataTypeDescriptor(), null, type);
    }
    
    @Test
    public void testSingleEquals() throws Exception {
        Index index = index("items", "sku");
        List<ExpressionNode> equals = Collections.singletonList(constant("0121", MString.VARCHAR.instance(false)));
        CostEstimate costEstimate = costEstimator.costIndexScan(index, equals,
                                                                null, false, null, false);
        assertEquals(113, costEstimate.getRowCount());

        equals = Collections.singletonList(constant("0123", MString.VARCHAR.instance(false)));
        costEstimate = costEstimator.costIndexScan(index, equals,
                                                   null, false, null, false);
        assertEquals(103, costEstimate.getRowCount());

        index = index("addresses", "state");
        equals = Collections.singletonList(constant(null, MString.VARCHAR.instance(false)));
        costEstimate = costEstimator.costIndexScan(index, equals,
                                                   null, false, null, false);
        assertEquals(13, costEstimate.getRowCount());
    }

    @Test
    public void testSingleRange() throws Exception {
        Index index = index("customers", "name");
        CostEstimate costEstimate = costEstimator.costIndexScan(index, null,
                                                                constant("M", MString.VARCHAR.instance(false)), true, 
                                                                constant("N", MString.VARCHAR.instance(false)), false); // LIKE 'M%'.
        assertEquals(4, costEstimate.getRowCount());
        costEstimate = costEstimator.costIndexScan(index, null,
                                                   constant("L", MString.VARCHAR.instance(false)), true, 
                                                   constant("Q", MString.VARCHAR.instance(false)), true); // BETWEEN 'L' AND 'Q'
        assertEquals(17, costEstimate.getRowCount());
        // Lower bound only
        costEstimate = costEstimator.costIndexScan(index, null,
                                                   constant("B", MString.VARCHAR.instance(false)), true, null, false); // >= 'B'
        // 5 rows from entry with key Ctewy. "B" is near the beginning of this range which contains 6 keys total.
        // The remainder of the histogram has 90 rows.
        assertEquals(95, costEstimate.getRowCount());
        // Upper bound only
        costEstimate = costEstimator.costIndexScan(index, null,
                                                   null, false, constant("B", MString.VARCHAR.instance(false)), false); // < 'B'
        // 1 rows from entry with key Ctewy. "B" is near the beginning of this range which contains 6 keys total.
        // 4 rows from the prior entries.
        assertEquals(5, costEstimate.getRowCount());
    }

    @Test
    public void testVariableEquals() throws Exception {
        Index index = index("customers", "PRIMARY");
        List<ExpressionNode> equals = Collections.singletonList(variable(MString.VARCHAR.instance(false)));
        CostEstimate costEstimate = costEstimator.costIndexScan(index, equals,
                                                                null, false, null, false);
        assertEquals(1, costEstimate.getRowCount());

        index = index("customers", "name");
        costEstimate = costEstimator.costIndexScan(index, equals,
                                                   null, false, null, false);
        assertEquals(1, costEstimate.getRowCount());
    }
    
    @Test
    public void testMultipleIndexEqEq() {
        Index index = groupIndex("sku_and_date");
        List<ExpressionNode> bothEQ = Arrays.asList(constant("0254", MString.VARCHAR.instance(false)),
                                                    constant(1032274, MDateAndTime.DATE.instance(false)));
        CostEstimate costEstimate = costEstimator.costIndexScan(index, bothEQ, null, false, null, false);
        // sku 0254 is a match for a histogram entry with eq = 110. total distinct count = 20000
        //     selectivity = 110 / 20000 = 0.0055
        // date 1032274 is covered by histogram entry with key = 1032275, distinct = 43, lt = 59, total distinct count = 1000
        //     selectivity = 59 / (43 * 1000) = 0.0014
        // Combined selectivity = 7.55e-6
        // Expected rows = 1
        assertEquals(1, costEstimate.getRowCount());
    }

    @Test
    public void testMultipleIndexEqRange() {
        Index index = groupIndex("sku_and_date");
        List<ExpressionNode> skuEQ = Arrays.asList(constant("0254", MString.VARCHAR.instance(false)));
        ExpressionNode loDate = constant(1029500, MDateAndTime.DATE.instance(false));
        ExpressionNode hiDate = constant(1033000, MDateAndTime.DATE.instance(false));
        CostEstimate costEstimate = costEstimator.costIndexScan(index, skuEQ, loDate, true, hiDate, true);
        // sku 0254 is a match for a histogram entry with eq = 110. total distinct count = 20000
        //     selectivity = 110 / 20000 = 0.0055
        // date 1029500 (2010-11-28):
        //     - Past the first 3 histogram entries (1029263, 1029270, 1029298)
        //     - 32% through the range of the 4th entry (1029937) with distinct = 108, lt = 140, so this contributes a
        //       about 140 * (1 - 0.32) = 96. Actual bit-twiddly calculation yields 107.
        // date 1033000 (2017-09-08):
        //     - Next entries cover 541  entries
        //     - 69% through the entry with key 1033431 with distinct = 99, lt = 127, so it contributes
        //       127 * 0.69 = 88, (actually 46)
        // Selectivity for the date range is (107 + 541 + 46) / 1000 = 69.4%
        // Combined selectivity = 0.00382
        // Expected rows = 76.
        assertEquals(76, costEstimate.getRowCount());
        // No upper bound on date
        // 107 from entry containing low bound (as above), 799 from higher entries, 906/1000 = 0.906 selectivity
        // Combined selectivity = 0.004983
        // Expected rows = 100
        costEstimate = costEstimator.costIndexScan(index, skuEQ, loDate, true, null, false);
        assertEquals(100, costEstimate.getRowCount());
        // No lower bound on date
        // 46 from entry containing hi bound (as above), 742 from higher entries, 788/1000 = 0.788 selectivity
        // Combined selectivity = 0.004334
        // Expected rows = 100
        costEstimate = costEstimator.costIndexScan(index, skuEQ, null, false, hiDate, true);
        assertEquals(87, costEstimate.getRowCount());
    }

    /* Cardinalities are:
     *   100 customers, 1000 (10x) orders, 20000 (20x) items, 100 (1x) addresses 
     */

    @Test
    public void testI2COI() throws Exception {
        TableSource c = tableSource("customers");
        TableSource o = tableSource("orders");
        TableSource i = tableSource("items");
        CostEstimate costEstimate = costFlatten(i, Arrays.asList(c, o, i));
        assertEquals(1, costEstimate.getRowCount());
        assertEquals(199.3716,
                     costEstimate.getCost(),
                     0.0001);
    }

    @Test
    public void testO2COI() throws Exception {
        TableSource c = tableSource("customers");
        TableSource o = tableSource("orders");
        TableSource i = tableSource("items");
        CostEstimate costEstimate = costFlatten(o, Arrays.asList(c, o, i));
        assertEquals(20, costEstimate.getRowCount());
        assertEquals(1791.5738000000001,
                     costEstimate.getCost(),
                     0.0001);
    }

    @Test
    public void testC2COI() throws Exception {
        TableSource c = tableSource("customers");
        TableSource o = tableSource("orders");
        TableSource i = tableSource("items");
        CostEstimate costEstimate = costFlatten(c, Arrays.asList(c, o, i));
        assertEquals(200, costEstimate.getRowCount());
        assertEquals(16915.8916,
                     costEstimate.getCost(),
                     0.0001);
    }

    @Test
    public void testO2I() throws Exception {
        TableSource o = tableSource("orders");
        TableSource i = tableSource("items");
        CostEstimate costEstimate = costFlatten(o, Arrays.asList(i));
        assertEquals(20, costEstimate.getRowCount());
        assertEquals(826.3822,
                     costEstimate.getCost(),
                     0.0001);
    }

    @Test
    public void testI2COIA() throws Exception {
        TableSource c = tableSource("customers");
        TableSource o = tableSource("orders");
        TableSource i = tableSource("items");
        TableSource a = tableSource("addresses");
        CostEstimate costEstimate = costFlatten(i, Arrays.asList(c, o, i, a));
        assertEquals(1, costEstimate.getRowCount());
        assertEquals(297.094,
                     costEstimate.getCost(),
                     0.0001);
    }

    @Test
    public void testI2A() throws Exception {
        TableSource i = tableSource("items");
        TableSource a = tableSource("addresses");
        CostEstimate costEstimate = costFlatten(i, Arrays.asList(a));
        assertEquals(1, costEstimate.getRowCount());
        assertEquals(7.7224,
                     costEstimate.getCost(),
                     0.0001);
    }

    @Test
    public void testA2I() throws Exception {
        TableSource a = tableSource("addresses");
        TableSource i = tableSource("items");
        CostEstimate costEstimate = costFlatten(a, Arrays.asList(i));
        assertEquals(200, costEstimate.getRowCount());
        assertEquals(8026.3822,
                     costEstimate.getCost(),
                     0.0001);
    }

    @Test
    public void testA2COIS() throws Exception {
        TableSource c = tableSource("customers");
        TableSource o = tableSource("orders");
        TableSource i = tableSource("items");
        TableSource a = tableSource("addresses");
        TableSource s = tableSource("shipments");
        CostEstimate costEstimate = costFlatten(a, Arrays.asList(c, o, i, s));
        assertEquals(300, costEstimate.getRowCount());
        assertEquals(21412.2794,
                     costEstimate.getCost(),
                     0.0001);
    }

    private CostEstimate costFlatten(TableSource indexTable,
                                     Collection<TableSource> requiredTables) {
        TableGroup tableGroup = new TableGroup(indexTable.getTable().getTable().getGroup());
        indexTable.setGroup(tableGroup);
        Map<Table,TableSource> tableSources = new HashMap<>();
        tableSources.put(indexTable.getTable().getTable(), indexTable);
        for (TableSource table : requiredTables) {
            tableSources.put(table.getTable().getTable(), table);
            table.setGroup(tableGroup);
        }
        for (Table childTable : new ArrayList<>(tableSources.keySet())) {
            TableSource childSource = tableSources.get(childTable);
            while (true) {
                Join parentJoin = childTable.getParentJoin();
                if (parentJoin == null) break;
                Table parentTable = parentJoin.getParent();
                TableSource parentSource = tableSources.get(parentTable);
                if (parentSource == null) {
                    parentSource = new TableSource(tree.addNode(parentTable), true,
                                                   parentTable.getName().getTableName());
                    tableSources.put(parentTable, parentSource);
                }
                TableGroupJoin groupJoin = new TableGroupJoin(tableGroup, 
                                                              parentSource, childSource,
                                                              Collections.<ComparisonCondition>emptyList(),
                                                              parentJoin);
                childTable = parentTable;
                childSource = parentSource;
            }
        }
        List<TableSource> orderedSources = new ArrayList<>(tableSources.values());
        Collections.sort(orderedSources, tableSourceById);
        Map<TableSource,TableGroupJoinNode> nodes =
            new HashMap<>();
        TableGroupJoinNode root = null;
        for (TableSource tableSource : orderedSources) {
            nodes.put(tableSource, new TableGroupJoinNode(tableSource));
        }
        for (TableSource childSource : orderedSources) {
            TableGroupJoinNode childNode = nodes.get(childSource);
            TableSource parentSource = childSource.getParentTable();
            if (parentSource == null) {
                root = childNode;
                continue;
            }
            TableGroupJoinNode parentNode = nodes.get(parentSource);
            childNode.setParent(parentNode);
            childNode.setNextSibling(parentNode.getFirstChild());
            parentNode.setFirstChild(childNode);
        }
        TableGroupJoinTree joinTree = new TableGroupJoinTree(root);
        return costEstimator.costFlatten(joinTree, indexTable, 
                                         new HashSet<>(requiredTables));
    }

    static final Comparator<TableSource> tableSourceById = new Comparator<TableSource>() {
        @Override
        // Access things in stable order.
        public int compare(TableSource t1, TableSource t2) {
            return t1.getTable().getTable().getTableId().compareTo(t2.getTable().getTable().getTableId());
        }
    };

    @Test
    public void testUniformPortion() {
        assertEquals(8, CostEstimator.uniformPortion("A".getBytes(),
                                                     "Z".getBytes(),
                                                     "C".getBytes(),
                                                     100));
        assertEquals(48, CostEstimator.uniformPortion("A".getBytes(),
                                                      "Z".getBytes(),
                                                      "M".getBytes(),
                                                      100));
        assertEquals(76, CostEstimator.uniformPortion("A".getBytes(),
                                                      "Z".getBytes(),
                                                      "T".getBytes(),
                                                      100));
        assertEquals(50,  CostEstimator.uniformPortion(new byte[] { (byte)0x00 },
                                                       new byte[] { (byte)0x00, (byte)0x00, (byte)0xFF, (byte)0xFF },
                                                       new byte[] { (byte)0x00, (byte)0x00, (byte)0x80, (byte)0x00 },
                                                       100));
    }
}
