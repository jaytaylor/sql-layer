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

package com.akiban.sql.optimizer.rule;

import com.akiban.ais.model.*;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.types.AkType;
import com.akiban.sql.optimizer.OptimizerTestBase;
import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.rule.costmodel.CostModel;
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
        costModel = CostModel.newCostModel(schema, costEstimator);
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
        List<ExpressionNode> equals = Collections.singletonList(constant(key, AkType.INT));
        return costEstimator.costIndexScan(index, equals, null, false, null, false);
    }
    
    private CostEstimate costIntersect(CostEstimate x, CostEstimate y, long outRows)
    {
        return new CostEstimate(outRows, costModel.intersect((int) x.getRowCount(), (int) y.getRowCount())); 
    }

    private CostEstimate costAncestorLookup(UserTableRowType rowType, long nRows)
    {
        return new CostEstimate(nRows, nRows * costModel.ancestorLookup(Arrays.asList(rowType)));
    }
    
    private CostEstimate costSelect(CostEstimate in, long outRows)
    {
        return new CostEstimate(outRows, costModel.select((int) in.getRowCount()));
    }
    
    private UserTableRowType rowType(String tableName)
    {
        return schema.userTableRowType(ais.getUserTable(SCHEMA, tableName));
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
        return tree.addNode((UserTable)table(name));
    }

    protected TableSource tableSource(String name) {
        return new TableSource(tableNode(name), true, name);
    }

    protected static ExpressionNode constant(Object value, AkType type) {
        return new ConstantExpression(value, type);
    }

    protected static ExpressionNode variable(AkType type) {
        return new ParameterExpression(0, null, type, null);
    }

    static final Comparator<TableSource> tableSourceById = new Comparator<TableSource>() {
        @Override
        // Access things in stable order.
        public int compare(TableSource t1, TableSource t2) {
            return t1.getTable().getTable().getTableId().compareTo(t2.getTable().getTable().getTableId());
        }
    };
}
