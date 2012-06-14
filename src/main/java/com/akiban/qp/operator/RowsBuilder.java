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

package com.akiban.qp.operator;

import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.DerivedTypesSchema;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.ValuesRowType;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.Strings;

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

    public RowsBuilder row(Object... values) {
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
        InternalRow row = new InternalRow(rowType, Arrays.asList(values));
        rows.add(row);
        return this;
    }

    public RowsBuilder(AkType... types) {
        this.rowType = new ValuesRowType(null, COUNT.incrementAndGet(), types);
        this.types = types;
    }

    public RowsBuilder(DerivedTypesSchema schema, AkType... types) {
        this.rowType = schema.newValuesType(types);
        this.types = types;
    }

    public RowsBuilder(RowType rowType) {
        this.rowType = rowType;
        types = new AkType[this.rowType.nFields()];
        for (int i=0; i < types.length; ++i) {
            types[i] = this.rowType.typeAt(i);
        }
    }

    // object state

    private final RowType rowType;
    private final AkType[] types;
    private final Deque<Row> rows = new ArrayDeque<Row>();

    // class state

    private static final AtomicInteger COUNT = new AtomicInteger();

    // nested classes

    private static class InternalRow extends AbstractRow {
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

        @Override
        public String toString() {
            return Strings.join(values, ", ");
        }

        private InternalRow(RowType rowType, List<? extends ValueSource> values) {
            this.rowType = rowType;
            this.values = new ArrayList<ValueSource>(values);
        }

        private final RowType rowType;
        private final List<ValueSource> values;
    }
}
