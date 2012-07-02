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

package com.akiban.qp.persistitadapter.sort;

import com.akiban.qp.operator.*;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.PersistitValueValueSource;
import com.akiban.server.PersistitValueValueTarget;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.std.LiteralExpression;
import com.akiban.server.service.session.Session;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.util.tap.InOutTap;
import com.persistit.*;
import com.persistit.exception.PersistitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Sorter
{
    public Sorter(QueryContext context,
                  Cursor input, 
                  RowType rowType, 
                  API.Ordering ordering,
                  API.SortOption sortOption,
                  InOutTap loadTap)
        throws PersistitException
    {
        this.context = context;
        this.adapter = (PersistitAdapter)context.getStore();
        this.input = input;
        this.rowType = rowType;
        this.ordering = ordering.copy();
        String sortTreeName = SORT_TREE_NAME_PREFIX + SORTER_ID_GENERATOR.getAndIncrement();
        this.exchange = exchange(adapter, sortTreeName);
        this.key = exchange.getKey();
        this.keyTarget = new PersistitKeyValueTarget();
        this.keyTarget.attach(this.key);
        this.value = exchange.getValue();
        this.valueTarget = new PersistitValueValueTarget();
        this.valueTarget.attach(this.value);
        this.rowFields = rowType.nFields();
        this.fieldTypes = new AkType[this.rowFields];
        for (int i = 0; i < rowFields; i++) {
            fieldTypes[i] = rowType.typeAt(i);
        }
        preserveDuplicates = sortOption == API.SortOption.PRESERVE_DUPLICATES;
        if (preserveDuplicates) {
            // Append a count field as a sort key, to ensure key uniqueness for Persisit. By setting
            // the ascending flag equal to that of some other sort field, we don't change an all-ASC or all-DESC sort
            // into a less efficient mixed-mode sort.
            this.ordering.append(DUMMY_EXPRESSION, ordering.ascending(0));
        }
        int nsort = this.ordering.sortColumns();
        this.evaluations = new ArrayList<ExpressionEvaluation>(nsort);
        this.orderingTypes = new AkType[nsort];
        for (int i = 0; i < nsort; i++) {
            orderingTypes[i] = this.ordering.type(i);
            ExpressionEvaluation evaluation = this.ordering.expression(i).evaluation();
            evaluation.of(context);
            evaluations.add(evaluation);
        }
        this.loadTap = loadTap;
    }

    public Cursor sort() throws PersistitException
    {
        loadTree();
        return cursor();
    }

    public void close()
    {
        if (exchange != null) {
            try {
                TempVolumeState tempVolumeState = adapter.getSession().get(TEMP_VOLUME_STATE);
                int sortsInProgress = tempVolumeState.endSort();
                if (sortsInProgress == 0) {
                    // Returns disk space used by the volume
                    tempVolumeState.volume().close();
                    adapter.getSession().remove(TEMP_VOLUME_STATE);
                }
            } catch (PersistitException e) {
                adapter.handlePersistitException(e);
            } finally {
                // Don't return the exchange. TreeServiceImpl caches it for the tree, and we're done with the tree.
                // THIS CAUSES A LEAK OF EXCHANGES: adapter.returnExchange(exchange);
                exchange = null;
            }
        }
    }

    private void loadTree() throws PersistitException
    {
        try {
            loadTap.in();
            try {
                Row row = input.next();
                while (row != null) {
                    context.checkQueryCancelation();
                    createKey(row);
                    createValue(row);
                    exchange.store();
                    loadTap.out();
                    loadTap.in();
                    row = input.next();
                }
            } finally {
                loadTap.out();
            }
        } catch (PersistitException e) {
            LOG.error("Caught exception while loading tree for sort", e);
            exchange.removeAll();
            adapter.handlePersistitException(e);
        }
    }

    private Cursor cursor()
    {
        exchange.clear();
        return SortCursor.create(context, null, ordering, new SorterIterationHelper());
    }

    private void createKey(Row row)
    {
        key.clear();
        int sortFields = ordering.sortColumns() - (preserveDuplicates ? 1 : 0);
        for (int i = 0; i < sortFields; i++) {
            ExpressionEvaluation evaluation = evaluations.get(i);
            evaluation.of(row);
            ValueSource keySource = evaluation.eval();
            keyTarget.expectingType(orderingTypes[i], row.rowType().collatorAt(i));
            Converters.convert(keySource, keyTarget);
        }
        if (preserveDuplicates) {
            key.append(rowCount++);
        }
    }

    private void createValue(Row row)
    {
        value.clear();
        value.setStreamMode(true);
        for (int i = 0; i < rowFields; i++) {
            ValueSource field = row.eval(i);
            valueTarget.expectingType(fieldTypes[i]);
            Converters.convert(field, valueTarget);
        }
    }

    private static Exchange exchange(PersistitAdapter adapter, String treeName) throws PersistitException
    {
        Session session = adapter.getSession();
        Persistit persistit = adapter.persistit().getDb();
        TempVolumeState tempVolumeState = session.get(TEMP_VOLUME_STATE);
        if (tempVolumeState == null) {
            // Persistit creates a temp volume per "Persistit session", and these are currently one-to-one with threads.
            // Conveniently, server sessions and threads are also one-to-one. If either of these relationships ever
            // change, then the use of session resources and temp volumes will need to be revisited. But for now,
            // persistit.createTemporaryVolume creates a temp volume that is private to the persistit session and
            // therefore to the server session.
            Volume volume = persistit.createTemporaryVolume();
            tempVolumeState = new TempVolumeState(volume);
            session.put(TEMP_VOLUME_STATE, tempVolumeState);
        }
        tempVolumeState.startSort();
        return new Exchange(persistit, tempVolumeState.volume(), treeName, true);
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(Sorter.class);
    private static final Expression DUMMY_EXPRESSION = LiteralExpression.forNull();
    private static final String SORT_TREE_NAME_PREFIX = "sort.";
    private static final AtomicLong SORTER_ID_GENERATOR = new AtomicLong(0);
    private static final Session.Key<TempVolumeState> TEMP_VOLUME_STATE = Session.Key.named("TEMP_VOLUME_STATE");

    // Object state

    final PersistitAdapter adapter;
    final Cursor input;
    final RowType rowType;
    final API.Ordering ordering;
    final boolean preserveDuplicates;
    final List<ExpressionEvaluation> evaluations;
    final QueryContext context;
    final Key key;
    final Value value;
    final PersistitKeyValueTarget keyTarget;
    final PersistitValueValueTarget valueTarget;
    final int rowFields;
    final AkType fieldTypes[], orderingTypes[];
    Exchange exchange;
    long rowCount = 0;
    private final InOutTap loadTap;

    // Inner classes

    private class SorterIterationHelper implements IterationHelper
    {
        @Override
        public Row row()
        {
            ValuesHolderRow row = new ValuesHolderRow(rowType);
            value.setStreamMode(true);
            for (int i = 0; i < rowFields; i++) {
                ValueHolder valueHolder = row.holderAt(i);
                valueSource.expectedType(fieldTypes[i]);
                valueHolder.copyFrom(valueSource);
            }
            return row;
        }

        @Override
        public void close()
        {
            Sorter.this.close();
        }

        @Override
        public Exchange exchange()
        {
            return exchange;
        }

        SorterIterationHelper()
        {
            valueSource = new PersistitValueValueSource();
            valueSource.attach(value);
        }

        private final PersistitValueValueSource valueSource;
    }

    // public so that tests can see it
    public static class TempVolumeState
    {
        public TempVolumeState(Volume volume)
        {
            this.volume = volume;
            sortsInProgress = 0;
        }

        public Volume volume()
        {
            return volume;
        }

        public void startSort()
        {
            sortsInProgress++;
        }

        public int endSort()
        {
            sortsInProgress--;
            assert sortsInProgress >= 0;
            return sortsInProgress;
        }

        private final Volume volume;
        private int sortsInProgress;
    }
}
