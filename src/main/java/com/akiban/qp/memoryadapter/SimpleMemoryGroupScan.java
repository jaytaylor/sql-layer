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

package com.akiban.qp.memoryadapter;

import com.akiban.ais.model.TableName;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

import java.util.Iterator;

public abstract class SimpleMemoryGroupScan<T> implements MemoryGroupCursor.GroupScan {

    protected abstract void eval(int field, T data, PValueTarget target);
    protected abstract void eval(int field, T data, ValueTarget target);

    @Override
    public Row next() {
        if (!iterator.hasNext())
            return null;
        return new InternalRow(iterator.next());
    }

    @Override
    public void close() {
        // nothing
    }

    public SimpleMemoryGroupScan(MemoryAdapter adapter, TableName tableName, Iterator<? extends T> iterator) {
        this.iterator = iterator;
        this.rowType = adapter.schema().userTableRowType(adapter.schema().ais().getUserTable(tableName));
        if (Types3Switch.ON) {
            pValues = new PValue[rowType.nFields()];
            values = null;
        }
        else {
            pValues = null;
            values = new ValueHolder[rowType.nFields()];
        }
    }

    private class InternalRow extends AbstractRow {

        @Override
        public PValueSource pvalue(int i) {
            PValue value = pValues[i];
            SimpleMemoryGroupScan.this.eval(i, data, value);
            return value;
        }

        @Override
        public ValueSource eval(int i) {
            ValueHolder value = values[i];
            SimpleMemoryGroupScan.this.eval(i, data, value);
            return value;
        }

        @Override
        public RowType rowType() {
            return rowType;
        }

        @Override
        public HKey hKey() {
            throw new UnsupportedOperationException();
        }

        private InternalRow(T data) {
            this.data = data;
        }

        private final T data;
    }

    private final Iterator<? extends T> iterator;
    private final RowType rowType;
    private final PValue[] pValues;
    private final ValueHolder[] values;
}
