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

package com.akiban.sql.optimizer.rule.cost;

import com.akiban.ais.model.*;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.types.AkType;
import com.akiban.sql.optimizer.OptimizerTestBase;
import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.rule.RulesTestHelper;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.*;

import static java.lang.Math.min;
import static java.lang.Math.pow;
import static java.lang.Math.round;

@Ignore
public class UnorderedIntersectCostSensitivityTest
{
    @Before
    public void loadSchema() throws Exception
    {
        ais = OptimizerTestBase.parseSchema(new File(RESOURCE_DIR, "schema.ddl"));
        RulesTestHelper.ensureRowDefs(ais);
        tree = new TableTree();
        schema = new Schema(ais);
        t = table("t");
        idxAOrdered = index("t", "idx_a_orderby");
        idxBUnordered = index("t", "idx_b");
        idxCUnordered = index("t", "idx_c");
        tRowType = schema.userTableRowType(t);
        idxBRowType = schema.indexRowType(idxBUnordered);
        idxCRowType = schema.indexRowType(idxCUnordered);
        costEstimator = new TestCostEstimator(ais, schema, new File(RESOURCE_DIR, "stats.yaml"), false, new Properties());
        costModel = CostModel.newCostModel(schema, costEstimator);
    }

    @Test
    public void test() throws Exception
    {
        for (int limit : LIMITS) {
            double[][] scanProbeCost = new double[A_KEYS.length][];
            double[][] scanFilterCost = new double[A_KEYS.length][];
            double[][] intersectProbeSortCost = new double[A_KEYS.length][];
            Plan[][] winners = new Plan[A_KEYS.length][];
            for (int a = 0; a < A_KEYS.length; a++) {
                scanProbeCost[a] = new double[BC_KEYS.length];
                scanFilterCost[a] = new double[BC_KEYS.length];
                intersectProbeSortCost[a] = new double[BC_KEYS.length];
                winners[a] = new Plan[BC_KEYS.length];
                for (int bc = 0; bc < BC_KEYS.length; bc++) {
                    scanProbeCost[a][bc] = scanProbe(limit, a, bc, bc);
                    scanFilterCost[a][bc] = scanFilter(limit, a, bc, bc);
                    intersectProbeSortCost[a][bc] = intersectProbeSort(limit, a, bc, bc);
                    winners[a][bc] =
                        scanProbeCost[a][bc] < min(scanFilterCost[a][bc], intersectProbeSortCost[a][bc])
                        ? Plan.SCAN_PROBE :
                        scanFilterCost[a][bc] < min(scanProbeCost[a][bc], intersectProbeSortCost[a][bc])
                        ? Plan.SCAN_FILTER
                        : Plan.INTERSECT_PROBE_SORT;
                }
            }
            print(String.format("limit %s, winner", limit), winners);
            System.out.println();
            print(String.format("limit %s, scanProbe (%s)", limit, Plan.SCAN_PROBE), scanProbeCost, "%,11d");
            System.out.println();
            print(String.format("limit %s, scanFilter (%s)", limit, Plan.SCAN_FILTER), scanFilterCost, "%,11d");
            System.out.println();
            print(String.format("limit %s, intersectProbeSort (%s)", limit, Plan.INTERSECT_PROBE_SORT), intersectProbeSortCost, "%,11d");
            System.out.println();
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            System.out.println();
        }
    }

    private double scanProbe(int limit, int a, int b, int c)
    {
        // Scan t.a index.
        // For each qualifying row, group lookup to evaluate predicates on b, c.
        // Stop when limit reached.
        double aSelectivity = selectivity(A_KEYS, a);
        double bSelectivity = selectivity(BC_KEYS, b);
        double cSelectivity = selectivity(BC_KEYS, c);
        // Cost if the query is run without the LIMIT clause
        CostEstimate costAScan = costIndexScan(idxAOrdered, A_KEYS[a]);
        long aRows = costAScan.getRowCount();
        double lookup = rowLookup() * aRows;
        double bSelect = costModel.select((int) aRows);
        double cSelect = costModel.select((int) aRows);
        double selectCost = costAScan.getCost() + lookup + bSelect + cSelect;
        // Scale back based on limit
        long rows = round(aSelectivity * bSelectivity * cSelectivity * ROWS);
        if (rows > limit) {
            selectCost *= (double) limit / rows;
            rows = limit;
        }
        double outputCost = selectCost + costModel.project(tRowType.nFields(), (int) rows);
        return outputCost;
    }

    private double scanFilter(int limit, int a, int b, int c)
    {
        // Scan t.b and t.c indexes, forming bloom filters.
        // Scan t.a index. For each row, check bloom filter.
        // If filter returns positive, retrieve a row.
        double aSelectivity = selectivity(A_KEYS, a);
        double bSelectivity = selectivity(BC_KEYS, b);
        double cSelectivity = selectivity(BC_KEYS, c);
        // Time to load t.b bloom filter. Assume that extraction of hkey and loading costs the same as selection.
        int bRows = (int) (ROWS * bSelectivity);
        double bScan = costModel.indexScan(idxBRowType, bRows);
        double bFilter = bScan + costModel.select(bRows);
        // Time to load t.c bloom filter.
        int cRows = (int) (ROWS * cSelectivity);
        double cScan = costModel.indexScan(idxCRowType, cRows);
        double cFilter = cScan + costModel.select(cRows);
        // Scan t.a index
        CostEstimate costAScan = costIndexScan(idxAOrdered, A_KEYS[a]);
        // Check all qualifying rows from the t.a scan. Assume filtering cost the same as selection.
        double filterA = costAScan.getCost() + costModel.select((int) costAScan.getRowCount());
        // Scale back filtering step based on limit
        long rows = round(aSelectivity * bSelectivity * cSelectivity * ROWS);
        if (rows > limit) {
            filterA *= (double) limit / rows;
            rows = limit;
        }
        double outputCost = bFilter + cFilter + filterA + costModel.project(tRowType.nFields(), (int) rows);
/*
        System.out.println(String.format(
            "OF: limit = %s, a = %s, b = %s: post-filter: %s",
            limit, a, b, filterA + costModel.project(tRowType, (int) rows)));
*/
        return outputCost;
    }

    private double intersectProbeSort(int limit, int a, int b, int c)
    {
        // Scan t.b forming bloom filter.
        // Scan t.c, check filter.
        // For hkey from t.c that passes filter, probe t, selecting based on t.a.
        // Sort.
        double aSelectivity = selectivity(A_KEYS, a);
        double bSelectivity = selectivity(BC_KEYS, b);
        double cSelectivity = selectivity(BC_KEYS, c);
        // Time to load t.b bloom filter. Assume that extraction of hkey and loading costs the same as selection.
        int bRows = (int) (ROWS * bSelectivity);
        double bScan = costModel.indexScan(idxBRowType, bRows);
        double bFilter = bScan + costModel.select(bRows);
        // Time to scan t.c index and check filter.
        int cRows = (int) (ROWS * cSelectivity);
        double cScan = costModel.indexScan(idxCRowType, cRows);
        double cFilter = cScan + costModel.select(cRows);
        // Probe t and check t.a condition
        int bcRows = (int) (bSelectivity * cSelectivity * ROWS);
        double probeA = rowLookup() * bcRows + costModel.select(bcRows);
        // Sort
        int rows = (int) round(aSelectivity * bcRows);
        double sort =
            limit <= MAX_ROWS_FOR_SORT_INSERTION_LIMITED
            ? costModel.sortWithLimit(rows, 1)
            : costModel.sort(rows, false);
        // Project as many rows as required by limit
        double outputCost = bFilter + cFilter + probeA + sort + costModel.project(tRowType.nFields(), min(rows, limit));
/*
        System.out.println(String.format(
            "IPS: limit = %s, a = %s, b = %s: post-filter: %s",
            limit, a, b, probeA + sort + costModel.project(tRowType, min(rows, limit))));
*/
        return outputCost;
    }

    private CostEstimate costIndexScan(Index index, int key)
    {
        List<ExpressionNode> equals = Collections.singletonList(constant(key, AkType.INT));
        return costEstimator.costIndexScan(index, equals, null, false, null, false);
    }

    private double rowLookup()
    {
        return costAncestorLookup(tRowType, 1).getCost();
    }

    private CostEstimate costAncestorLookup(UserTableRowType rowType, long nRows)
    {
        return new CostEstimate(nRows, nRows * costModel.ancestorLookup(Arrays.asList(rowType)));
    }

    private double selectivity(int[] keys, int key)
    {
        return (double) (1 << keys[key]) / ROWS;
    }

    private String repeat(char c, int n)
    {
        StringBuilder buffer = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            buffer.append(c);
        }
        return buffer.toString();
    }

    private UserTable table(String name)
    {
        return (UserTable) ais.getTable(SCHEMA, name);
    }

    private Index index(String table, String name)
    {
        return table(table).getIndex(name);
    }

    private static ExpressionNode constant(Object value, AkType type)
    {
        return new ConstantExpression(value, type);
    }

    private void print(String label, double[][] data, String numberFormat)
    {
        System.out.println(label);
        System.out.print("a\\bc       ");
        for (int bc = 0; bc < BC_KEYS.length; bc++) {
            System.out.print(String.format("%11.3f ", pow(selectivity(BC_KEYS, bc), 2)));
        }
        System.out.println();
        System.out.print("          ");
        System.out.println(repeat('-', 1 + 12 * (BC_KEYS.length + 1)));
        for (int a = 0; a < A_KEYS.length; a++) {
            System.out.print(String.format("%9.3f |", selectivity(A_KEYS, a)));
            for (int bc = 0; bc < BC_KEYS.length; bc++) {
                System.out.print(String.format(String.format("%s ", numberFormat), (long) data[a][bc]));
            }
            System.out.println();
        }
    }

    private void print(String label, Plan[][] winners)
    {
        System.out.println(label);
        System.out.print("a\\bc       ");
        for (int bc = 0; bc < BC_KEYS.length; bc++) {
            System.out.print(String.format("%11.3f ", pow(selectivity(BC_KEYS, bc), 2)));
        }
        System.out.println();
        System.out.print("          ");
        System.out.println(repeat('-', 1 + 12 * (BC_KEYS.length + 1)));
        for (int a = 0; a < A_KEYS.length; a++) {
            System.out.print(String.format("%9.3f |", selectivity(A_KEYS, a)));
            for (int bc = 0; bc < BC_KEYS.length; bc++) {
                System.out.print(String.format("%11s ", winners[a][bc]));
            }
            System.out.println();
        }
    }

    public static final File RESOURCE_DIR = new File(OptimizerTestBase.RESOURCE_DIR, "unordered_index_intersection");
    public static final String SCHEMA = OptimizerTestBase.DEFAULT_SCHEMA;

    private final int MAX_KEY = 23;
    private final int ROWS = (1 << (MAX_KEY + 1)) - 1;
    // a keys:
    // - a = 2: 4/16M
    // ...
    // - a = 22: 4M/16M = 25%
    private final int[] A_KEYS = new int[]{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22};
    // B/C keys:
    // - b = 13, c = 13: (8K/16M)**2
    // ...
    // - b = 23, c = 23: (8M/16M)**2 = 25%
    private final int[] BC_KEYS = new int[]{13, 15, 17, 19, 21, 23};
    // limits:
    private final int[] LIMITS = new int[]{
        1 << 0,  // 1
        1 << 3,  // 8
        1 << 6,  // 64
        1 << 9,  // 512
        1 << 12, // 4K
        1 << 15, // 32K
        1 << 18, // 256K
        1 << 21, // 2M
        1 << 24, // 16M
    };
    private final int MAX_ROWS_FOR_SORT_INSERTION_LIMITED = 100;

    private AkibanInformationSchema ais;
    private TableTree tree;
    private CostEstimator costEstimator;
    private CostModel costModel;
    private Schema schema;
    private UserTable t;
    private Index idxAOrdered;
    private Index idxBUnordered;
    private Index idxCUnordered;
    private UserTableRowType tRowType;
    private IndexRowType idxBRowType;
    private IndexRowType idxCRowType;

    enum Plan {
        SCAN_PROBE("OP"),
        SCAN_FILTER("OF"),
        INTERSECT_PROBE_SORT("IPS");

        public String toString()
        {
            return symbol;
        }

        private Plan(String symbol)
        {
            this.symbol = symbol;
        }

        private final String symbol;
    }

}
