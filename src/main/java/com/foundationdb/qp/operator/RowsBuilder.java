/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.qp.operator;

import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.DerivedTypesSchema;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.ValuesRowType;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.FromObjectValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.util.ValueHolder;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.Types3Switch;
import com.foundationdb.server.types3.pvalue.PValue;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueSources;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.Strings;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public final class RowsBuilder {

    public Deque<Row> rows() {
        return rows;
    }

    public RowType rowType() {
        return rowType;
    }

    public boolean usingPValues() {
        return (tinsts != null);
    }

    public RowsBuilder row(Object... values) {
        if (usingPValues()) {
            PValueSource[] pvalues = new PValueSource[values.length];
            for (int i = 0; i < values.length; ++i) {
                PValueSource psource = (values[i] == null) ?
                        PValueSources.getNullSource(tinsts[i]) :
                        PValueSources.fromObject(values[i], null).value();
                if (!psource.tInstance().equalsExcludingNullable(tinsts[i])) {
                    // This assumes that anything that doesn't match is a string.
                    TExecutionContext context = new TExecutionContext(null,
                                                                      tinsts[i],
                                                                      null);
                    PValue pvalue = new PValue(tinsts[i]);
                    tinsts[i].typeClass().fromObject(context, psource, pvalue);
                    psource = pvalue;
                }
                pvalues[i] = psource;
            }
            return row(pvalues);
        }
        else {
            ValueHolder[] holders = new ValueHolder[values.length];
            FromObjectValueSource tmpSource = new FromObjectValueSource();
            for (int i = 0; i < values.length; ++i) {
                ValueHolder holder = new ValueHolder();
                tmpSource.setExplicitly(values[i], types[i]);
                holder.copyFrom(tmpSource);
                holders[i] = holder;
            }
            return row(holders);
        }
    }

    public RowsBuilder row(ValueSource... values) {
        ArgumentValidation.isEQ("values.length", values.length, types.length);
        for (int i=0; i < values.length; ++i) {
            ValueSource value = values[i];
            AkType valueType = value.getConversionType();
            AkType requiredType = types[i];
            if (valueType != AkType.NULL && valueType != requiredType) {
                throw new IllegalArgumentException("type at " + i + " must be " + requiredType + ", is " + valueType);
            }
        }
        InternalValuesRow row = new InternalValuesRow(rowType, Arrays.asList(values));
        rows.add(row);
        return this;
    }

    public RowsBuilder row(PValueSource... pvalues) {
        ArgumentValidation.isEQ("values.length", pvalues.length, tinsts.length);
        for (int i=0; i < pvalues.length; ++i) {
            PValueSource pvalue = pvalues[i];
            TInstance valueType = pvalue.tInstance();
            TInstance requiredType = tinsts[i];
            if (!valueType.equalsExcludingNullable(requiredType)) {
                throw new IllegalArgumentException("type at " + i + " must be " + requiredType + ", is " + valueType);
            }
        }
        InternalPValuesRow row = new InternalPValuesRow(rowType, Arrays.asList(pvalues));
        rows.add(row);
        return this;
    }

    public RowsBuilder(AkType... types) {
        this.rowType = new ValuesRowType(null, COUNT.incrementAndGet(), types);
        this.types = types;
        this.tinsts = null;
    }

    public RowsBuilder(DerivedTypesSchema schema, AkType... types) {
        this.rowType = schema.newValuesType(types);
        this.types = types;
        this.tinsts = null;
    }

    public RowsBuilder(TInstance... tinsts) {
        this.rowType = new ValuesRowType(null, COUNT.incrementAndGet(), tinsts);
        this.tinsts = tinsts;
        this.types = null;
    }

    public RowsBuilder(DerivedTypesSchema schema, TInstance... tinsts) {
        this.rowType = schema.newValuesType(tinsts);
        this.tinsts = tinsts;
        this.types = null;
    }

    public RowsBuilder(RowType rowType) {
        this.rowType = rowType;
        if (Types3Switch.ON) {
            tinsts = new TInstance[this.rowType.nFields()];
            for (int i=0; i < tinsts.length; ++i) {
                tinsts[i] = this.rowType.typeInstanceAt(i);
            }
            types = null;
        }
        else {
            types = new AkType[this.rowType.nFields()];
            for (int i=0; i < types.length; ++i) {
                types[i] = this.rowType.typeAt(i);
            }
            tinsts = null;
        }
    }

    // object state

    private final RowType rowType;
    private final AkType[] types;
    private final TInstance[] tinsts;
    private final Deque<Row> rows = new ArrayDeque<>();

    // class state

    private static final AtomicInteger COUNT = new AtomicInteger();

    // nested classes

    private static class InternalValuesRow extends AbstractRow {
        @Override
        public RowType rowType() {
            return rowType;
        }

        @Override
        public HKey hKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ValueSource eval(int i) {
            return values.get(i);
        }

        // Note that this also handles pvalue(int) by converting.

        @Override
        public String toString() {
            return Strings.join(values, ", ");
        }

        private InternalValuesRow(RowType rowType, List<? extends ValueSource> values) {
            this.rowType = rowType;
            this.values = new ArrayList<>(values);
        }

        private final RowType rowType;
        private final List<ValueSource> values;
    }

    private static class InternalPValuesRow extends AbstractRow {
        @Override
        public RowType rowType() {
            return rowType;
        }

        @Override
        public HKey hKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public PValueSource pvalue(int i) {
            return pvalues.get(i);
        }

        @Override
        public String toString() {
            return Strings.join(pvalues, ", ");
        }

        private InternalPValuesRow(RowType rowType, List<? extends PValueSource> pvalues) {
            this.rowType = rowType;
            this.pvalues = new ArrayList<>(pvalues);
        }

        private final RowType rowType;
        private final List<PValueSource> pvalues;
    }

}
