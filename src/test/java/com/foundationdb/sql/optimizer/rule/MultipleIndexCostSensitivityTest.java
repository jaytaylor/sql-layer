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

package com.foundationdb.sql.optimizer.rule;

import com.foundationdb.ais.model.*;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.sql.optimizer.OptimizerTestBase;
import com.foundationdb.sql.optimizer.plan.*;
import com.foundationdb.sql.optimizer.rule.cost.CostEstimator;
import com.foundationdb.sql.optimizer.rule.cost.CostModel;
import com.foundationdb.sql.optimizer.rule.cost.TestCostEstimator;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import static java.lang.Math.round;

@Ignore
public class MultipleIndexCostSensitivityTest
{
    public static final File RESOURCE_DIR = new File(OptimizerTestBase.RESOURCE_DIR, "1_vs_2_indexes");
    public static final String SCHEMA = OptimizerTestBase.DEFAULT_SCHEMA;

    private final int MAX_KEY = 16;
    private final int ROWS = (1 << (MAX_KEY + 1)) - 1;

    protected AkibanInformationSchema ais;
    protected TableTree tree;
    protected CostEstimator costEstimator;
    protected CostModel costModel;
    protected Schema schema;
    protected Index px;
    protected Index cy;

    @Before
    public void loadSchema() throws Exception {
        ais = OptimizerTestBase.parseSchema(new File(RESOURCE_DIR, "schema.ddl"));
        RulesTestHelper.ensureRowDefs(ais);
        tree = new TableTree();
        schema = new Schema(ais);
        px = index("parent", "px");
        cy = index("child", "cy");
        costEstimator = new TestCostEstimator(ais, schema, new File(RESOURCE_DIR, "stats.yaml"), false, new Properties());
        costModel = costEstimator.getCostModel();
    }

    @Test
    public void test() throws Exception
    {
        double[][] ratio = new double[MAX_KEY + 1][];
        double[][] twoIndexCost = new double[MAX_KEY + 1][];
        double[][] oneIndexCost = new double[MAX_KEY + 1][];
        for (int y = 0; y <= MAX_KEY; y++) {
            ratio[y] = new double[MAX_KEY + 1];
            twoIndexCost[y] = new double[MAX_KEY + 1];
            oneIndexCost[y] = new double[MAX_KEY + 1];
            for (int x = 0; x <= MAX_KEY; x++) {
                CostEstimate oneIndexOneSelect = oneIndexOneSelect(x, y);
                CostEstimate twoIndexes = twoIndexes(x, y);
                double costRatio = twoIndexes.getCost() / oneIndexOneSelect.getCost();
/*
                System.out.println(String.format("y = %s, x = %s:\toneIndexOneSelect = %s\ttwoIndexes = %s\tratio: %s",
                                                 y, x, oneIndexOneSelect, twoIndexes, costRatio));
*/
                ratio[y][x] = costRatio;
                twoIndexCost[y][x] = twoIndexes.getCost();
                oneIndexCost[y][x] = oneIndexOneSelect.getCost();
            }
        }
        print("ratio", ratio, "%9.3f");
        print("2 index cost", twoIndexCost, "%9.0f");
        print("1 index cost", oneIndexCost, "%9.0f");
    }

    private void print(String label, double[][] ratio, String numberFormat)
    {
        System.out.println(label);
        System.out.print("y\\x        ");
        for (int x = 0; x <= MAX_KEY; x++) {
            System.out.print(String.format("%9.3f ", selectivity(x)));
        }
        System.out.println();
        System.out.print("          ");
        System.out.println(repeat('-', 1 + 10 * (MAX_KEY + 1)));
        for (int y = 0; y <= MAX_KEY; y++) {
            System.out.print(String.format("%9.3f |", selectivity(y)));
            for (int x = 0; x <= MAX_KEY; x++) {
                System.out.print(String.format(String.format("%s ", numberFormat), ratio[y][x]));
            }
            System.out.println();
        }
    }

    CostEstimate oneIndexOneSelect(int x, int y)
    {
        CostEstimate costCy = costIndexScan(cy, y);
        CostEstimate costCyP = costAncestorLookup(rowType("parent"), costCy.getRowCount());
        CostEstimate costSelect = costSelect(costCyP, xyRows(x, y));
        return new CostEstimate(costSelect.getRowCount(), costCy.getCost() + costCyP.getCost() + costSelect.getCost());
    }

    CostEstimate twoIndexes(int x, int y)
    {
        CostEstimate costCy = costIndexScan(cy, y);
        CostEstimate costPx = costIndexScan(px, x);
        CostEstimate costIntersect = costIntersect(costCy, costPx, (int) xyRows(x, y));
        CostEstimate costLookup = costAncestorLookup(rowType("parent"), costIntersect.getRowCount());
        return new CostEstimate(costLookup.getRowCount(), costCy.getCost() + costPx.getCost() + costIntersect.getCost() + costLookup.getCost());
    }
    
    private CostEstimate costIndexScan(Index index, int key)
    {
        List<ExpressionNode> equals = Collections.singletonList(constant(key));
        return costEstimator.costIndexScan(index, equals, null, false, null, false);
    }
    
    private CostEstimate costIntersect(CostEstimate x, CostEstimate y, long outRows)
    {
        return new CostEstimate(outRows, costModel.intersect((int) x.getRowCount(), (int) y.getRowCount())); 
    }

    private CostEstimate costAncestorLookup(TableRowType rowType, long nRows)
    {
        return new CostEstimate(nRows, nRows * costModel.ancestorLookup(Arrays.asList(rowType)));
    }
    
    private CostEstimate costSelect(CostEstimate in, long outRows)
    {
        return new CostEstimate(outRows, costModel.select((int) in.getRowCount()));
    }
    
    private TableRowType rowType(String tableName)
    {
        return schema.tableRowType(ais.getTable(SCHEMA, tableName));
    }
    
    private long xyRows(int x, int y)
    {
        return round(selectivity(x) * selectivity(y) * ROWS);
    }
    
    private double selectivity(int key)
    {
        return (double) (1 << key) / ROWS;
    }

    private String repeat(char c, int n)
    {
        StringBuilder buffer = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            buffer.append(c);
        }
        return buffer.toString();
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

    protected static ExpressionNode constant(Object value) {
        return new ConstantExpression(value, MNumeric.BIGINT.instance(true));
    }

    protected static ExpressionNode variable() {
        return new ParameterExpression(0, null, null, null);
    }

    static final Comparator<TableSource> tableSourceById = new Comparator<TableSource>() {
        @Override
        // Access things in stable order.
        public int compare(TableSource t1, TableSource t2) {
            return t1.getTable().getTable().getTableId().compareTo(t2.getTable().getTable().getTableId());
        }
    };
}
