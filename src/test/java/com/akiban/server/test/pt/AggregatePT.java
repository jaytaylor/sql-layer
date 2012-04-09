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

package com.akiban.server.test.pt;

import com.akiban.server.test.ApiTestBase;

import com.akiban.ais.model.TableIndex;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.service.functions.FunctionsRegistryImpl;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class AggregatePT extends ApiTestBase {
    public static final int ROW_COUNT = 10000;
    public static final int WARMUPS = 10, REPEATS = 10;

    public AggregatePT() {
        super("PT");
    }

    private TableIndex index;

    @Before
    public void loadData() {
        int t = createTable("user", "t",
                            "id INT NOT NULL PRIMARY KEY",
                            "gid INT",
                            "flag BOOLEAN",
                            "sval VARCHAR(20) NOT NULL",
                            "n1 INT",
                            "n2 INT",
                            "k INT");
        Random rand = new Random(69);
        for (int i = 0; i < ROW_COUNT; i++) {
            writeRow(t, i,
                     rand.nextInt(10),
                     (rand.nextInt(100) < 80) ? 0 : 1,
                     randString(rand, 20),
                     rand.nextInt(100),
                     rand.nextInt(1000),
                     rand.nextInt());
        }
        index = createIndex("user", "t", "t_i", 
                            "gid", "sval", "flag", "k", "n1", "n2", "id");
    }

    private String randString(Random rand, int size) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < size; i++) {
            str.append((char)('A' + rand.nextInt(26)));
        }
        return str.toString();
    }

    @Test
    public void normalOperators() {
        Schema schema = new Schema(rowDefCache().ais());;
        IndexRowType indexType = schema.indexRowType(index);
        IndexKeyRange keyRange = IndexKeyRange.unbounded(indexType);
        API.Ordering ordering = new API.Ordering();
        ordering.append(new FieldExpression(indexType, 0), true);

        FunctionsRegistry functions = new FunctionsRegistryImpl();
        ExpressionComposer and = functions.composer("and");
        Expression pred1 = functions.composer("greaterOrEquals")
            .compose(Arrays.asList(Expressions.field(indexType, 1),
                                   Expressions.literal("M")));
        Expression pred2 = functions.composer("lessOrEquals")
            .compose(Arrays.asList(Expressions.field(indexType, 1),
                                   Expressions.literal("Y")));
        Expression pred = and.compose(Arrays.asList(pred1, pred2));
        pred2 = functions.composer("notEquals")
            .compose(Arrays.asList(Expressions.field(indexType, 2),
                                   Expressions.literal(1L)));
        pred = and.compose(Arrays.asList(pred, pred2));
        
        Operator plan = API.indexScan_Default(indexType, keyRange, ordering);
        RowType rowType = indexType;
        plan = API.select_HKeyOrdered(plan, rowType, pred);
        plan = API.project_Default(plan, rowType,
                                   Arrays.asList(Expressions.field(rowType, 0),
                                                 Expressions.field(rowType, 3),
                                                 Expressions.field(rowType, 4),
                                                 Expressions.field(rowType, 5)));
        rowType = plan.rowType();
        plan = API.aggregate_Partial(plan, rowType, 
                                     1, functions,
                                     Arrays.asList("count", "sum", "sum"));

        PersistitAdapter adapter = persistitAdapter(schema);
        QueryContext queryContext = queryContext(adapter);
        
        double time = 0.0;
        for (int i = 0; i < WARMUPS+REPEATS; i++) {
            long start = System.nanoTime();
            Cursor cursor = API.cursor(plan, queryContext);
            cursor.open();
            while (true) {
                Row row = cursor.next();
                if (row == null) break;
                if (i == 0) System.out.println(row);
            }
            cursor.close();
            long end = System.nanoTime();
            if (i >= WARMUPS)
                time += (end - start) / 1.0e6;
        }
        System.out.println(String.format("%g ms", time / REPEATS));
    }

    @Test
    public void pojoAggregator() throws PersistitException {
        double time = 0.0;
        for (int i = 0; i < WARMUPS+REPEATS; i++) {
            long start = System.nanoTime();
            POJOAggregator aggregator = new POJOAggregator(i == 0);
            Exchange exchange = persistitStore().getExchange(session(), index);
            exchange.clear();
            exchange.append(Key.BEFORE);
            while (exchange.traverse(Key.GT, true)) {
                Key key = exchange.getKey();
                aggregator.aggregate(key);
            }
            aggregator.emit();
            persistitStore().releaseExchange(session(), exchange);
            long end = System.nanoTime();
            if (i >= WARMUPS)
                time += (end - start) / 1.0e6;
        }
        System.out.println(String.format("%g ms", time / REPEATS));
    }

    private static class POJOAggregator {
        private boolean reset = true;
        
        private boolean key_init;
        private long key;
        private long count1;
        private boolean sum1_init;
        private long sum1;
        private boolean sum2_init;
        private long sum2;

        private final boolean doPrint;

        public POJOAggregator(boolean doPrint) {
            this.doPrint = doPrint;
        }

        public void aggregate(Key row) {
            row.indexTo(1);
            String sval = row.decodeString();
            if (("M".compareTo(sval) > 0) ||
                ("Y".compareTo(sval) < 0))
                return;
            row.indexTo(2);
            long flag = row.decodeLong();
            if (flag == 1)
                return;
            row.indexTo(0);
            long nextKey = row.decodeLong();
            if (!key_init) {
                key_init = true;
                key = nextKey;
            }
            else if (key != nextKey) {
                emit();
                key = nextKey;
                reset = true;
            }
            if (reset) {
                sum1_init = sum2_init = false;
                count1 = sum1 = sum2 = 0;
                reset = false;
            }
            row.indexTo(3);
            if (!row.isNull()) {
                count1++;
            }
            row.indexTo(4);
            if (!row.isNull()) {
                if (!sum1_init)
                    sum1_init = true;
                sum1 += row.decodeLong();
            }
            row.indexTo(5);
            if (!row.isNull()) {
                if (!sum2_init)
                    sum2_init = true;
                sum2 += row.decodeLong();
            }
        }

        public void emit() {
            if (doPrint)
                System.out.println(this);
        }

        @Override
        public String toString() {
            return String.format("%d: [%d %d %d]", key, count1, sum1, sum2);
        }

    }

}
