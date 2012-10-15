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
import com.akiban.qp.exec.Plannable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.OperatorExecutionBase;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.DerivedTypesSchema;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.ValuesRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.error.QueryCanceledException;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.std.BoundFieldExpression;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.expression.std.ExpressionTypes;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.service.functions.FunctionsRegistryImpl;
import com.akiban.server.service.session.Session;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;

import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;

public class AggregatePT extends ApiTestBase {
    public static final int NKEYS = 10;
    public static final int ROW_COUNT = 100000;
    public static final int WARMUPS = 100, REPEATS = 10;

    public AggregatePT() {
        super("PT");
    }

    private TableIndex index;

    @Before
    public void loadData() throws Exception {
        transactionally(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
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
                      rand.nextInt(NKEYS),
                      (rand.nextInt(100) < 80) ? 0 : 1,
                      randString(rand, 20),
                      rand.nextInt(100),
                      rand.nextInt(1000),
                      rand.nextInt());
         }
         index = createIndex("user", "t", "t_i", 
                             "gid", "sval", "flag", "k", "n1", "n2", "id");
         return null;
      }});
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
        Schema schema = new Schema(rowDefCache().ais());
        IndexRowType indexType = schema.indexRowType(index);
        IndexKeyRange keyRange = IndexKeyRange.unbounded(indexType);
        API.Ordering ordering = new API.Ordering();
        ordering.append(new FieldExpression(indexType, 0), true);
        
        Operator plan = API.indexScan_Default(indexType, keyRange, ordering);
        RowType rowType = indexType;

        plan = spa(plan, rowType);

        PersistitAdapter adapter = persistitAdapter(schema);
        QueryContext queryContext = queryContext(adapter);
        
        System.out.println("NORMAL OPERATORS");
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

    private Operator spa(Operator plan, RowType rowType) {
        FunctionsRegistry functions = new FunctionsRegistryImpl();
        ExpressionComposer and = functions.composer("and");
        Expression pred1 = functions.composer("greaterOrEquals")
            .compose(Arrays.asList(Expressions.field(rowType, 1),
                                   Expressions.literal("M")),
                     Arrays.asList(ExpressionTypes.varchar(20),
                                   ExpressionTypes.varchar(1),
                                   ExpressionTypes.BOOL));
        Expression pred2 = functions.composer("lessOrEquals")
            .compose(Arrays.asList(Expressions.field(rowType, 1),
                                   Expressions.literal("Y")),
                     Arrays.asList(ExpressionTypes.varchar(20),
                                   ExpressionTypes.varchar(1),
                                   ExpressionTypes.BOOL));
        Expression pred = and.compose(Arrays.asList(pred1, pred2), 
                                      Arrays.asList(ExpressionTypes.BOOL, ExpressionTypes.BOOL, ExpressionTypes.BOOL));
        pred2 = functions.composer("notEquals")
            .compose(Arrays.asList(Expressions.field(rowType, 2),
                                   Expressions.literal(1L)),
                     Arrays.asList(ExpressionTypes.LONG,
                                   ExpressionTypes.LONG,
                                   ExpressionTypes.BOOL));
        pred = and.compose(Arrays.asList(pred, pred2),
                           Arrays.asList(ExpressionTypes.BOOL, ExpressionTypes.BOOL, ExpressionTypes.BOOL));
        
        plan = API.select_HKeyOrdered(plan, rowType, pred);
        plan = API.project_Default(plan, rowType,
                                   Arrays.asList(Expressions.field(rowType, 0),
                                                 Expressions.field(rowType, 3),
                                                 Expressions.field(rowType, 4),
                                                 Expressions.field(rowType, 5)));
        rowType = plan.rowType();
        plan = API.aggregate_Partial(plan, rowType, 
                                     1, functions,
                                     Arrays.asList("count", "sum", "sum"),
                                     new ArrayList<Object>(3));
        return plan;
    }

    @Test
    public void bespokeOperator() {
        Schema schema = new Schema(rowDefCache().ais());
        IndexRowType indexType = schema.indexRowType(index);
        IndexKeyRange keyRange = IndexKeyRange.unbounded(indexType);
        API.Ordering ordering = new API.Ordering();
        ordering.append(new FieldExpression(indexType, 0), true);
        
        Operator plan = API.indexScan_Default(indexType, keyRange, ordering);
        RowType rowType = indexType;

        plan = new BespokeOperator(plan);

        PersistitAdapter adapter = persistitAdapter(schema);
        QueryContext queryContext = queryContext(adapter);
        
        System.out.println("BESPOKE OPERATOR");
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
        System.out.println("POJO");
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

    static class BespokeOperator extends Operator {
        private Operator inputOperator;
        private RowType outputType;

        public BespokeOperator(Operator inputOperator) {
            this.inputOperator = inputOperator;
            outputType = new BespokeRowType();
        }

        @Override
        protected Cursor cursor(QueryContext context) {
            return new BespokeCursor(context, API.cursor(inputOperator, context), outputType);
        }

        @Override
        public void findDerivedTypes(Set<RowType> derivedTypes) {
            inputOperator.findDerivedTypes(derivedTypes);
            derivedTypes.add(outputType);
        }

        @Override
        public List<Operator> getInputOperators() {
            return Collections.singletonList(inputOperator);
        }

        @Override
        public RowType rowType() {
            return outputType;
        }

        @Override
        public CompoundExplainer getExplainer(ExplainContext context) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    static class BespokeCursor extends OperatorExecutionBase implements Cursor {
        private Cursor inputCursor;
        private RowType outputType;
        private ValuesHolderRow outputRow;
        private BespokeAggregator aggregator;

        public BespokeCursor(QueryContext context, Cursor inputCursor, RowType outputType) {
            super(context);
            this.inputCursor = inputCursor;
            this.outputType = outputType;
        }

        @Override
        public void open() {
            inputCursor.open();
            outputRow = new ValuesHolderRow(outputType);
            aggregator = new BespokeAggregator();
        }

        @Override
        public void close() {
            inputCursor.close();
            aggregator = null;
        }

        @Override
        public void destroy() {
            close();
            inputCursor = null;
        }

        @Override
        public boolean isIdle() {
            return ((inputCursor != null) && (aggregator == null));
        }

        @Override
        public boolean isActive() {
            return ((inputCursor != null) && (aggregator != null));
        }

        @Override
        public boolean isDestroyed() {
            return (inputCursor == null);
        }

        @Override
        public Row next() {
            if (aggregator == null)
                return null;
            while (true) {
                Row inputRow = inputCursor.next();
                if (inputRow == null) {
                    if (aggregator.isEmpty()) {
                        close();
                        return null;
                    }
                    aggregator.fill(outputRow);
                    close();
                    return outputRow;
                }
                if (aggregator.aggregate(inputRow, outputRow)) {
                    return outputRow;
                }
            }
        }
    }

    static final AkType[] TYPES = { 
        AkType.LONG, AkType.LONG, AkType.LONG, AkType.LONG
    };
    
    static final TInstance[] TINSTANCES = {
        MNumeric.INT.instance(true),MNumeric.INT.instance(true) ,MNumeric.INT.instance(true) ,MNumeric.INT.instance(true)
    };

    static class BespokeRowType extends RowType {
        public BespokeRowType() {
            super(-1);
        }

        @Override
        public DerivedTypesSchema schema() {
            return null;
        }

        public int nFields() {
            return TYPES.length;
        }

        @Override
        public AkType typeAt(int index) {
            return TYPES[index];
        }

        @Override
        public TInstance typeInstanceAt(int index) {
            return TINSTANCES[index];
        }
    }

    static class BespokeAggregator {
        private boolean key_init;
        private long key;
        private long count1;
        private boolean sum1_init;
        private long sum1;
        private boolean sum2_init;
        private long sum2;

        public boolean isEmpty() {
            return !key_init;
        }

        public boolean aggregate(Row inputRow, ValuesHolderRow outputRow) {
            // The select part.
            String sval = inputRow.eval(1).getString();
            if (("M".compareTo(sval) > 0) ||
                ("Y".compareTo(sval) < 0))
                return false;
            long flag = inputRow.eval(2).getInt();
            if (flag == 1)
                return false;

            // The actual aggregate part.
            boolean emit = false, reset = false;
            long nextKey = inputRow.eval(0).getInt();
            if (!key_init) {
                key_init = reset = true;
                key = nextKey;
            }
            else if (key != nextKey) {
                fill(outputRow);
                emit = reset = true;
                key = nextKey;
            }
            if (reset) {
                sum1_init = sum2_init = false;
                count1 = sum1 = sum2 = 0;
            }
            ValueSource value = inputRow.eval(3);
            if (!value.isNull()) {
                count1++;
            }
            value = inputRow.eval(4);
            if (!value.isNull()) {
                if (!sum1_init)
                    sum1_init = true;
                sum1 += value.getInt();
            }
            value = inputRow.eval(5);
            if (!value.isNull()) {
                if (!sum2_init)
                    sum2_init = true;
                sum2 += value.getInt();
            }
            return emit;
        }

        public void fill(ValuesHolderRow row) {
            row.holderAt(0).putLong(key);
            row.holderAt(1).putLong(count1);
            row.holderAt(2).putLong(sum1);
            row.holderAt(3).putLong(sum2);
        }

        @Override
        public String toString() {
            return String.format("%d: [%d %d %d]", key, count1, sum1, sum2);
        }

    }

    static class POJOAggregator {
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
            boolean reset = false;
            long nextKey = row.decodeLong();
            if (!key_init) {
                key_init = reset = true;
                key = nextKey;
            }
            else if (key != nextKey) {
                emit();
                reset = true;
                key = nextKey;
            }
            if (reset) {
                sum1_init = sum2_init = false;
                count1 = sum1 = sum2 = 0;
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

    @Test
    public void sorted() {
        Schema schema = new Schema(rowDefCache().ais());
        IndexRowType indexType = schema.indexRowType(index);
        IndexKeyRange keyRange = IndexKeyRange.unbounded(indexType);
        API.Ordering ordering = new API.Ordering();
        ordering.append(new FieldExpression(indexType, 0), true);
        
        Operator plan = API.indexScan_Default(indexType, keyRange, ordering);
        RowType rowType = indexType;

        plan = spa(plan, rowType);
        rowType = plan.rowType();
        
        ordering = new API.Ordering();
        ordering.append(new FieldExpression(rowType, 2), true);
        plan = API.sort_InsertionLimited(plan, rowType, ordering, 
                                         API.SortOption.PRESERVE_DUPLICATES, 100);
        
        PersistitAdapter adapter = persistitAdapter(schema);
        QueryContext queryContext = queryContext(adapter);
        
        System.out.println("SORTED");
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
    public void parallel() {
        Schema schema = new Schema(rowDefCache().ais());
        IndexRowType indexType = schema.indexRowType(index);
        ValuesRowType valuesType = schema.newValuesType(AkType.LONG, AkType.LONG);
        IndexBound lo = new IndexBound(new RowBasedUnboundExpressions(indexType, Collections.<Expression>singletonList(new BoundFieldExpression(0, new FieldExpression(valuesType, 0)))), new SetColumnSelector(0));
        IndexBound hi = new IndexBound(new RowBasedUnboundExpressions(indexType, Collections.<Expression>singletonList(new BoundFieldExpression(0, new FieldExpression(valuesType, 1)))), new SetColumnSelector(0));
        IndexKeyRange keyRange = IndexKeyRange.bounded(indexType, lo, true, hi, false);
        API.Ordering ordering = new API.Ordering();
        ordering.append(new FieldExpression(indexType, 0), true);
        
        Operator plan = API.indexScan_Default(indexType, keyRange, ordering);
        RowType rowType = indexType;

        plan = spa(plan, rowType);
        rowType = plan.rowType();

        int nthreads = Integer.parseInt(System.getProperty("test.nthreads", "4"));
        double n = (double)NKEYS / nthreads;
        List<ValuesRow> keyRows = new ArrayList<ValuesRow>();
        for (int i = 0; i < nthreads; i++) {
            Object[] values = new Object[2];
            values[0] = Math.round(n * i);
            values[1] = Math.round(n * (i+1));
            keyRows.add(new ValuesRow(valuesType, values));
        }
        plan = new Map_Parallel(plan, rowType, valuesType, keyRows, 0);
                                
        ordering = new API.Ordering();
        ordering.append(new FieldExpression(rowType, 2), true);
        plan = API.sort_InsertionLimited(plan, rowType, ordering, 
                                         API.SortOption.PRESERVE_DUPLICATES, 100);
        
        PersistitAdapter adapter = persistitAdapter(schema);
        QueryContext queryContext = queryContext(adapter);
        
        System.out.println("PARALLEL " + nthreads);
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

    class Map_Parallel extends Operator {
        private Operator inputOperator;
        private RowType outputType;
        private ValuesRowType valuesType;
        private List<ValuesRow> valuesRows;
        private int bindingPosition;

        public Map_Parallel(Operator inputOperator, RowType outputType, ValuesRowType valuesType, List<ValuesRow> valuesRows, int bindingPosition) {
            this.inputOperator = inputOperator;
            this.outputType = outputType;
            this.valuesType = valuesType;
            this.valuesRows = valuesRows;
            this.bindingPosition = bindingPosition;
        }

        @Override
        protected Cursor cursor(QueryContext context) {
            return new ParallelCursor(context, inputOperator, valuesType, valuesRows, bindingPosition);
        }

        @Override
        public List<Operator> getInputOperators() {
            return Collections.singletonList(inputOperator);
        }

        @Override
        public CompoundExplainer getExplainer(ExplainContext context) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    // Nulls are not allowed in ConcurrentLinkedQueue.
    static final Object EOF_ROW = new Object();

    static class RowQueue {
        private final Thread reader;
        private Queue<Object> queue;
        
        public RowQueue(Thread reader) {
            this.reader = reader;
            queue = new ConcurrentLinkedQueue<Object>();
        }
        
        public Row take() throws InterruptedException {
            while (true) {
                Object row = queue.poll();
                if (row == null) {
                    // assert (reader == Thread.currentThread());
                    LockSupport.park(this);
                }
                else {
                    return ((row != EOF_ROW) ? (Row)row : null);
                }
            }
        }

        public void put(Row row) throws InterruptedException {
            boolean added = queue.offer((row != null) ? row : EOF_ROW);
            // No size limit on ConcurrentLinkedQueue. A real implementation would
            // need to block sometimes when too full.
            assert added;
            LockSupport.unpark(reader);
        }

        public void clear() {
            queue.clear();
        }
    }

    class ParallelCursor extends OperatorExecutionBase implements Cursor {
        private RowQueue queue;
        private List<WorkerThread> threads;
        private int nrunning;
        private Row heldRow;

        public ParallelCursor(QueryContext context, Operator inputOperator, ValuesRowType valuesType, List<ValuesRow> valuesRows, int bindingPosition) {
            super(context);
            this.context = context;
            int nthreads = valuesRows.size();
            queue = new RowQueue(Thread.currentThread());
            threads = new ArrayList<WorkerThread>(nthreads);
            for (ValuesRow valuesRow : valuesRows) {
                threads.add(new WorkerThread(context, inputOperator, valuesType, valuesRow, bindingPosition, queue));
            }
        }

        @Override
        public void open() {
            nrunning = 0;
            for (WorkerThread thread : threads) {
                thread.open();
                nrunning++;
            }
        }

        @Override
        public void close() {
            if (heldRow != null) {
                heldRow.release();
                heldRow = null;
            }
            for (WorkerThread thread : threads) {
                if (thread.close())
                    nrunning--;
            }
            // TODO: Could be off if closed prematurely and there are
            // nulls in the queue (or waiting to be added).
            assert (nrunning == 0) : nrunning;
        }

        @Override
        public void destroy() {
            for (WorkerThread thread : threads) {
                thread.destroy();
            }
            threads = null;
            queue.clear();
            queue = null;
        }

        @Override
        public boolean isIdle() {
            return (nrunning == 0);
        }

        @Override
        public boolean isActive() {
            return (nrunning > 0);
        }

        @Override
        public boolean isDestroyed() {
            return (threads == null);
        }

        @Override
        public Row next() {
            while (nrunning > 0) {
                if (heldRow != null) {
                    heldRow.release();
                    heldRow = null;
                }
                try {
                    Row row = queue.take();
                    if (row != null)
                        heldRow = row; // Was acquired by worker.
                }
                catch (InterruptedException ex) {
                    throw new QueryCanceledException(context.getSession());
                }
                if (heldRow == null)
                    nrunning--;
                else
                    return heldRow;
            }
            return null;
        }
        
    }

    class WorkerThread implements Runnable {
        private Session session;
        private PersistitAdapter adapter;
        private QueryContext context;
        private Cursor inputCursor;        
        private RowQueue queue;
        private Thread thread;
        private volatile boolean open;
        
        public WorkerThread(QueryContext context, Operator inputOperator, ValuesRowType valuesType, ValuesRow valuesRow, int bindingPosition, RowQueue queue) {
            session = createNewSession();
            adapter = new PersistitAdapter((Schema)valuesType.schema(), store(), treeService(), session, configService());
            context = queryContext(adapter);
            context.setRow(bindingPosition, valuesRow);
            inputCursor = API.cursor(inputOperator, context);
            this.queue = queue;
        }
        
        public void open() {
            thread = new Thread(this);
            thread.start();
        }

        public boolean close() {
            if (!open) return false;
            thread.interrupt();
            //thread.join();
            return true;
        }

        public void destroy() {
            close();
            if (inputCursor != null) {
                inputCursor.destroy();
                inputCursor = null;
            }
            session.close();
        }

        @Override
        public void run() {
            Transaction transaction = treeService().getTransaction(session);
            inputCursor.open();
            open = true;
            try {
                transaction.begin();
                while (open) {
                    Row row = inputCursor.next();
                    if (row == null) 
                        open = false;
                    else
                        row.acquire();
                    queue.put(row);
                }
                transaction.commit();
            }
            catch (InterruptedException ex) {
                throw new QueryCanceledException(context.getSession());
            }
            catch (PersistitException ex) {
                PersistitAdapter.handlePersistitException(session, ex);
            } 
            finally {
                inputCursor.close();
                transaction.end();
            }
        }

    }

}
