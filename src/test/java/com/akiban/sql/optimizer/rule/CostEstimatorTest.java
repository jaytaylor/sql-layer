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

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.OptimizerTestBase;

import static com.akiban.sql.optimizer.rule.CostEstimator.*;

import com.akiban.sql.optimizer.plan.*;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.types.AkType;

import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;

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
        costEstimator = new TestCostEstimator(ais, new File(RESOURCE_DIR, "stats.yaml"), new Schema(ais));
    }

    protected Table table(String name) {
        return ais.getTable(SCHEMA, name);
    }

    protected Index index(String table, String name) {
        return table(table).getIndex(name);
    }

    protected TableNode tableNode(String name) {
        return tree.addNode((UserTable)table(name));
    }

    protected TableSource tableSource(String name) {
        return new TableSource(tableNode(name), true);
    }

    protected static ExpressionNode constant(Object value, AkType type) {
        return new ConstantExpression(value, type);
    }

    protected static ExpressionNode variable(AkType type) {
        return new ParameterExpression(0, null, type, null);
    }

    @Test
    public void testSingleEquals() throws Exception {
        Index index = index("items", "sku");
        List<ExpressionNode> equals = Collections.singletonList(constant("0121", AkType.VARCHAR));
        CostEstimate costEstimate = costEstimator.costIndexScan(index, equals,
                                                                null, false, null, false);
        assertEquals(113, costEstimate.getRowCount());

        equals = Collections.singletonList(constant("0123", AkType.VARCHAR));
        costEstimate = costEstimator.costIndexScan(index, equals,
                                                   null, false, null, false);
        assertEquals(103, costEstimate.getRowCount());

        index = index("addresses", "state");
        equals = Collections.singletonList(constant(null, AkType.NULL));
        costEstimate = costEstimator.costIndexScan(index, equals,
                                                   null, false, null, false);
        assertEquals(13, costEstimate.getRowCount());
    }

    @Test
    public void testSingleRange() throws Exception {
        Index index = index("customers", "name");
        CostEstimate costEstimate = costEstimator.costIndexScan(index, null,
                                                                constant("M", AkType.VARCHAR), true, constant("N", AkType.VARCHAR), false); // LIKE 'M%'.
        assertEquals(4, costEstimate.getRowCount());
        costEstimate = costEstimator.costIndexScan(index, null,
                                                   constant("L", AkType.VARCHAR), true, constant("Q", AkType.VARCHAR), true); // BETWEEN 'L' AND 'Q'
        assertEquals(17, costEstimate.getRowCount());
    }

    @Test
    public void testVariableEquals() throws Exception {
        Index index = index("customers", "PRIMARY");
        List<ExpressionNode> equals = Collections.singletonList(variable(AkType.VARCHAR));
        CostEstimate costEstimate = costEstimator.costIndexScan(index, equals,
                                                                null, false, null, false);
        assertEquals(1, costEstimate.getRowCount());

        index = index("customers", "name");
        costEstimate = costEstimator.costIndexScan(index, equals,
                                                   null, false, null, false);
        assertEquals(1, costEstimate.getRowCount());
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
        assertEquals(9469.0,
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
        assertEquals(11765.5468,
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
        assertEquals(35605.3382,
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
        assertEquals(10027.7584,
                     costEstimate.getCost(),
                     0.0001);
    }

    @Test
    public void testI2A() throws Exception {
        TableSource i = tableSource("items");
        TableSource a = tableSource("addresses");
        CostEstimate costEstimate = costFlatten(i, Arrays.asList(a));
        assertEquals(1, costEstimate.getRowCount());
        assertEquals(8807.7584,
                     costEstimate.getCost(),
                     0.0001);
    }

    @Test
    public void testA2I() throws Exception {
        TableSource a = tableSource("addresses");
        TableSource i = tableSource("items");
        CostEstimate costEstimate = costFlatten(a, Arrays.asList(i));
        assertEquals(200, costEstimate.getRowCount());
        // The customer random access doesn't actually happen in this
        // side-branch case, but that complexity isn't in the
        // estimation.
        assertEquals(19055.5468,
                     costEstimate.getCost(),
                     0.0001);
    }

    private CostEstimate costFlatten(TableSource indexTable,
                                     Collection<TableSource> requiredTables) {
        TableGroup tableGroup = new TableGroup(indexTable.getTable().getTable().getGroup());
        indexTable.setGroup(tableGroup);
        Map<UserTable,TableSource> tableSources = new HashMap<UserTable,TableSource>();
        tableSources.put(indexTable.getTable().getTable(), indexTable);
        for (TableSource table : requiredTables) {
            tableSources.put(table.getTable().getTable(), table);
            table.setGroup(tableGroup);
        }
        for (UserTable childTable : new ArrayList<UserTable>(tableSources.keySet())) {
            TableSource childSource = tableSources.get(childTable);
            while (true) {
                Join parentJoin = childTable.getParentJoin();
                if (parentJoin == null) break;
                UserTable parentTable = parentJoin.getParent();
                TableSource parentSource = tableSources.get(parentTable);
                if (parentSource == null) {
                    parentSource = new TableSource(tree.addNode(parentTable), true);
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
        Map<TableSource,TableGroupJoinTree.TableGroupJoinNode> nodes =
            new HashMap<TableSource,TableGroupJoinTree.TableGroupJoinNode>();
        TableGroupJoinTree.TableGroupJoinNode root = null;
        for (TableSource tableSource : tableSources.values()) {
            nodes.put(tableSource, new TableGroupJoinTree.TableGroupJoinNode(tableSource));
        }
        for (TableGroupJoinTree.TableGroupJoinNode childNode : nodes.values()) {
            TableSource childSource = childNode.getTable();
            TableSource parentSource = childSource.getParentTable();
            if (parentSource == null) {
                root = childNode;
                continue;
            }
            TableGroupJoinTree.TableGroupJoinNode parentNode = nodes.get(parentSource);
            childNode.setParent(parentNode);
            childNode.setNextSibling(parentNode.getFirstChild());
            parentNode.setFirstChild(childNode);
        }
        TableGroupJoinTree joinTree = new TableGroupJoinTree(root);
        return costEstimator.costFlatten(joinTree, indexTable, 
                                         new HashSet<TableSource>(requiredTables));
    }

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
