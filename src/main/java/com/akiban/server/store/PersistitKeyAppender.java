
package com.akiban.server.store;

import com.akiban.ais.model.Column;
import com.akiban.qp.util.PersistitKey;
import com.akiban.server.PersistitKeyPValueTarget;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDataPValueSource;
import com.akiban.server.rowdata.RowDataValueSource;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.persistit.Key;

public abstract class PersistitKeyAppender {

    public final void append(int value) {
        key.append(value);
    }

    public final void append(long value) {
        key.append(value);
    }

    public final void append(Object object, FieldDef fieldDef) {
        append(object, fieldDef.column());
    }

    public final void appendFieldFromKey(Key fromKey, int depth) {
        PersistitKey.appendFieldFromKey(key, fromKey, depth);
    }

    public final void appendNull() {
        key.append(null);
    }

    public final Key key() {
        return key;
    }

    public final void clear()
    {
        key().clear();
    }

    public abstract void append(Object object, Column column);

    public abstract void append(ValueSource source, Column column);

    public abstract void append(PValueSource source, Column column);

    public abstract void append(FieldDef fieldDef, RowData rowData);

    public static PersistitKeyAppender create(Key key) {
        return
            Types3Switch.ON
            ? new New(key)
            : new Old(key);
    }

    protected PersistitKeyAppender(Key key) {
        this.key = key;
    }

    protected final Key key;

    // Inner classes

    private static class Old extends PersistitKeyAppender
    {
        public Old(Key key) {
            super(key);
            fromRowDataSource = new RowDataValueSource();
            fromObjectSource = new FromObjectValueSource();
            target = new PersistitKeyValueTarget();
            target.attach(this.key);
        }

        public void append(Object object, Column column) {
            fromObjectSource.setReflectively(object);
            target.expectingType(column);
            Converters.convert(fromObjectSource, target);
        }

        public void append(ValueSource source, Column column) {
            target.expectingType(column);
            Converters.convert(source, target);
        }

        public void append(PValueSource source, Column column) {
            throw new UnsupportedOperationException();
        }

        public void append(FieldDef fieldDef, RowData rowData) {
            fromRowDataSource.bind(fieldDef, rowData);
            target.expectingType(fieldDef.column());
            Converters.convert(fromRowDataSource, target);
        }

        private final FromObjectValueSource fromObjectSource;
        private final RowDataValueSource fromRowDataSource;
        private final PersistitKeyValueTarget target;
    }

    private static class New extends PersistitKeyAppender
    {
        public New(Key key) {
            super(key);
            fromRowDataSource = new RowDataPValueSource();
            target = new PersistitKeyPValueTarget();
            target.attach(this.key);
        }

        public void append(Object object, Column column) {
            column.tInstance().writeCollating(PValueSources.fromObject(object, column.getType().akType()).value(), target);
        }

        public void append(ValueSource source, Column column) {
            throw new UnsupportedOperationException();
        }

        public void append(PValueSource source, Column column) {
            column.tInstance().writeCollating(source, target);
        }

        public void append(FieldDef fieldDef, RowData rowData) {
            fromRowDataSource.bind(fieldDef, rowData);
            Column column = fieldDef.column();
            column.tInstance().writeCollating(fromRowDataSource, target);
        }

        private final RowDataPValueSource fromRowDataSource;
        private final PersistitKeyPValueTarget target;
    }
}
