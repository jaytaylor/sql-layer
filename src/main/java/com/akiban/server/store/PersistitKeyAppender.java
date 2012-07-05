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

package com.akiban.server.store;

import com.akiban.ais.model.Column;
import com.akiban.qp.util.PersistitKey;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDataValueSource;
import com.akiban.server.types.AkType;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.FromObjectValueSource;
import com.persistit.Key;

public final class PersistitKeyAppender {

    public void append(int value) {
        key.append(value);
    }

    public void append(long value) {
        key.append(value);
    }

    public void append(Object object, Column column) {
        fromObjectSource.setReflectively(object);
        target.expectingType(column);
        Converters.convert(fromObjectSource, target);
    }

    public void append(Object object, FieldDef fieldDef) {
        append(object, fieldDef.column());
    }

    public void append(FieldDef fieldDef, RowData rowData) {
        fromRowDataSource.bind(fieldDef, rowData);
        target.expectingType(fieldDef.column());
        Converters.convert(fromRowDataSource, target);
    }

    public void appendNull() {
        target.expectingType(AkType.NULL).putNull();
    }

    public void appendFieldFromKey(Key fromKey, int depth)
    {
        PersistitKey.appendFieldFromKey(fromKey, depth, key);
    }

    public Key key() {
        return key;
    }

    public PersistitKeyAppender(Key key) {
        this.key = key;
        fromRowDataSource = new RowDataValueSource();
        fromObjectSource = new FromObjectValueSource();
        target = new PersistitKeyValueTarget();
        target.attach(this.key);
    }

    private final FromObjectValueSource fromObjectSource;
    private final RowDataValueSource fromRowDataSource;
    private final PersistitKeyValueTarget target;
    private final Key key;
}
