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

package com.akiban.qp.persistitadapter.sort;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.OperatorExecutionBase;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapterException;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.PersistitValueValueSource;
import com.akiban.server.PersistitValueValueTarget;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.std.LiteralExpression;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.util.ValueHolder;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Sorter
{
    public Sorter(PersistitAdapter adapter, Cursor input, RowType rowType, API.Ordering ordering, Bindings bindings)
    {
        this.adapter = adapter;
        this.input = input;
        // This typecast is pretty bad. But I really don't want to pass the query start time as an argument from
        // the Sort_Tree operator, through the StoreAdapter interface, to here.
        this.queryStartTimeMsec = ((OperatorExecutionBase) input).startTimeMsec();
        this.rowType = rowType;
        this.ordering = ordering.copy();
        this.bindings = bindings;
        this.exchange = adapter.takeExchangeForSorting
            (new SortTreeLink(SORT_TREE_NAME_PREFIX + SORTER_ID_GENERATOR.getAndIncrement()));
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
        // Append a count field as a sort key, to ensure key uniqueness for Persisit. By setting
        // the ascending flag equal to that of some other sort field, we don't change an all-ASC or all-DESC sort
        // into a less efficient mixed-mode sort.
        this.ordering.append(DUMMY_EXPRESSION, ordering.ascending(0));
        int nsort = this.ordering.sortColumns();
        this.evaluations = new ArrayList<ExpressionEvaluation>(nsort);
        this.orderingTypes = new AkType[nsort];
        for (int i = 0; i < nsort; i++) {
            orderingTypes[i] = this.ordering.type(i);
            ExpressionEvaluation evaluation = this.ordering.expression(i).evaluation();
            evaluation.of(adapter);
            evaluation.of(bindings);
            evaluations.add(evaluation);
        }
    }

    public Cursor sort() throws PersistitException
    {
        loadTree();
        return cursor();
    }

    void close()
    {
        if (exchange != null) {
            try {
                exchange.removeTree();
            } catch (PersistitException e) {
                throw new PersistitAdapterException(e);
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
            Row row;
            while ((row = input.next()) != null) {
                adapter.checkQueryCancelation(queryStartTimeMsec);
                createKey(row);
                createValue(row);
                exchange.store();
            }
        } catch (PersistitException e) {
            LOG.error("Caught exception while loading tree for sort", e);
            exchange.removeAll();
            throw e;
        }
    }

    private Cursor cursor()
    {
        exchange.clear();
        SortCursor cursor = SortCursor.create(adapter, null, ordering, new SorterIterationHelper());
        cursor.open(bindings);
        return cursor;
    }

    private void createKey(Row row)
    {
        key.clear();
        int sortFields = ordering.sortColumns() - 1; // Don't include the artificial count field
        for (int i = 0; i < sortFields; i++) {
            ExpressionEvaluation evaluation = evaluations.get(i);
            evaluation.of(row);
            ValueSource keySource = evaluation.eval();
            keyTarget.expectingType(orderingTypes[i]);
            Converters.convert(keySource, keyTarget);
        }
        key.append(rowCount++);
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

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(Sorter.class);
    private static final Expression DUMMY_EXPRESSION = LiteralExpression.forNull();
    private static final String SORT_TREE_NAME_PREFIX = "sort.";
    private static final AtomicLong SORTER_ID_GENERATOR = new AtomicLong(0);

    // Object state

    final PersistitAdapter adapter;
    final Cursor input;
    final RowType rowType;
    final API.Ordering ordering;
    final List<ExpressionEvaluation> evaluations;
    final Bindings bindings;
    final Key key;
    final Value value;
    final PersistitKeyValueTarget keyTarget;
    final PersistitValueValueTarget valueTarget;
    final int rowFields;
    final AkType fieldTypes[], orderingTypes[];
    Exchange exchange;
    long rowCount = 0;
    long queryStartTimeMsec;

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
}
