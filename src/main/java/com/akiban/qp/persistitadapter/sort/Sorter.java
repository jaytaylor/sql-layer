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

import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapterException;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.PersistitValueValueTarget;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.std.LiteralExpression;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sorter
{
    public Sorter(PersistitAdapter adapter, Cursor input, RowType rowType, API.Ordering ordering, Bindings bindings)
    {
        this.adapter = adapter;
        this.input = input;
        this.rowType = rowType;
        this.ordering = ordering.copy();
        this.bindings = bindings;
        this.exchange = adapter.takeExchangeForSorting();
        this.key = exchange.getKey();
        this.keyTarget = new PersistitKeyValueTarget();
        this.keyTarget.attach(this.key);
        this.value = exchange.getValue();
        this.valueTarget = new PersistitValueValueTarget();
        this.valueTarget.attach(this.value);
        this.rowFields = rowType.nFields();
        this.fieldTypes = new AkType[this.rowFields];
        // Append a count field as a sort key, to ensure key uniqueness for Persisit. By setting
        // the ascending flag equal to that of some other sort field, we don't change an all-ASC or all-DESC sort
        // into a less efficient mixed-mode sort.
        this.ordering.append(DUMMY_EXPRESSION, ordering.ascending(0));
    }

    public Cursor sort() throws PersistitException
    {
        loadTree();
        return cursor();
    }

    void close()
    {
        try {
            exchange.removeAll();
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
        adapter.returnExchange(exchange);
    }

    private void loadTree() throws PersistitException
    {
        exchange.removeAll(); // In case cleanup was somehow avoided on previous sort.
        try {
            Row row;
            while ((row = input.next()) != null) {
                createKey(row);
                createValue(row);
                exchange.store();
            }
        } catch (PersistitException e) {
            LOG.error("Caught exception while loading tree for sort", e);
            exchange.removeAll();
            throw e;
        } finally {
            exchange.getValue().setStreamMode(false);
            adapter.returnExchange(exchange);
        }
    }

    private Cursor cursor()
    {
        boolean allAscending = true;
        boolean allDescending = true;
        for (int i = 0; i < ordering.sortFields(); i++) {
            if (ordering.ascending(i)) {
                allDescending = false;
            } else {
                allAscending = false;
            }
        }
        SortCursor cursor = allAscending ? new SortCursorAscending(this) :
                            allDescending ? new SortCursorDescending(this) : new SortCursorMixedOrder(this);
        cursor.open(bindings);
        return cursor;
    }

    private void createKey(Row row)
    {
        key.clear();
        int sortFields = ordering.sortFields() - 1; // Don't include the artificial count field
        for (int i = 0; i < sortFields; i++) {
            ExpressionEvaluation evaluation = ordering.evaluation(i);
            evaluation.of(row);
            ValueSource keySource = evaluation.eval();
            // TODO: When ordering has server expressions instead of qp expressions: ordering.type(i));
            // TODO: Or even better: fieldTypes[i]
            keyTarget.expectingType(keySource.getConversionType());
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
            if (fieldTypes[i] == null) {
                fieldTypes[i] = field.getConversionType();
            }
            valueTarget.expectingType(fieldTypes[i]);
            Converters.convert(field, valueTarget);
        }
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(Sorter.class);
    private static final Expression DUMMY_EXPRESSION = LiteralExpression.forNull();

    // Object state

    final PersistitAdapter adapter;
    final Cursor input;
    final RowType rowType;
    final API.Ordering ordering;
    final Bindings bindings;
    final Exchange exchange;
    final Key key;
    final Value value;
    final PersistitKeyValueTarget keyTarget;
    final PersistitValueValueTarget valueTarget;
    final int rowFields;
    // TODO: Horrible hack. When we switch from qp.Expression to server.Expression, use Expression.valueType()
    final AkType fieldTypes[];
    long rowCount = 0;
}
