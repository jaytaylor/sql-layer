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

package com.akiban.server;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

import com.akiban.ais.model.Column;
import com.akiban.collation.CString;
import com.akiban.collation.CollatorFactory;
import com.akiban.qp.operator.Cursor;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSourceHelper;
import com.akiban.server.types.ValueTarget;
import com.akiban.util.ByteSource;
import com.persistit.Key;

public final class PersistitKeyValueTarget implements ValueTarget {

    /*
     * Very temporary kludge to test collection performance without all the
     * required support. Following is a map of column name to collation name,
     * e.g., "email" -> "en_US".
     */
    private final static Map<String, String> collationColumns = new HashMap<String, String>();

    static {
        final ResourceBundle bundle = ResourceBundle.getBundle("com.akiban.server.column_collation_map");
        for (final String key : bundle.keySet()) {
            collationColumns.put(key, bundle.getString(key));
        }
    }

    private String collationName = null;

    // PersistitKeyValueTarget interface

    public void attach(Key key) {
        this.key = key;
    }

    public PersistitKeyValueTarget expectingType(AkType type) {
        if (type == AkType.INTERVAL_MILLIS || type == AkType.INTERVAL_MONTH)
            throw new UnsupportedOperationException();
        this.type = type;
        return this;
    }

    public PersistitKeyValueTarget expectingType(Column column) {
        /*
         * Temporary very kludgey hack to marshal in the collation name through
         * an externally defined map of columnName->collation
         */
        collationName = collationColumns.get(column.getName());
        return expectingType(column.getType().akType());
    }

    // ValueTarget interface

    @Override
    public void putNull() {
        checkState(AkType.NULL);
        key.append(null);
        invalidate();
    }

    @Override
    public void putDate(long value) {
        checkState(AkType.DATE);
        key.append(value);
        invalidate();
    }

    @Override
    public void putDateTime(long value) {
        checkState(AkType.DATETIME);
        key.append(value);
        invalidate();
    }

    @Override
    public void putDecimal(BigDecimal value) {
        checkState(AkType.DECIMAL);
        key.append(value);
        invalidate();
    }

    @Override
    public void putDouble(double value) {
        checkState(AkType.DOUBLE);
        key.append(value);
        invalidate();
    }

    @Override
    public void putFloat(float value) {
        checkState(AkType.FLOAT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putInt(long value) {
        checkState(AkType.INT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putLong(long value) {
        checkState(AkType.LONG);
        key.append(value);
        invalidate();
    }

    @Override
    public void putString(String value) {
        checkState(AkType.VARCHAR);
        if (collationName != null) {
            key.append(new CString(value, CollatorFactory.getCollator(collationName)));
        } else {
            key.append(value);
        }
        invalidate();
    }

    @Override
    public void putText(String value) {
        checkState(AkType.TEXT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putTime(long value) {
        checkState(AkType.TIME);
        key.append(value);
        invalidate();
    }

    @Override
    public void putTimestamp(long value) {
        checkState(AkType.TIMESTAMP);
        key.append(value);
        invalidate();
    }

    @Override
    public void putInterval_Millis(long value) {
        throw new UnsupportedOperationException("interval not supported yet");
    }

    @Override
    public void putInterval_Month(long value) {
        throw new UnsupportedOperationException("interval not supported yet");
    }

    @Override
    public void putUBigInt(BigInteger value) {
        checkState(AkType.U_BIGINT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putUDouble(double value) {
        checkState(AkType.U_DOUBLE);
        key.append(value);
        invalidate();
    }

    @Override
    public void putUFloat(float value) {
        checkState(AkType.U_FLOAT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putUInt(long value) {
        checkState(AkType.U_INT);
        key.append(value);
        invalidate();
    }

    @Override
    public void putVarBinary(ByteSource value) {
        checkState(AkType.VARBINARY);
        key().appendByteArray(value.byteArray(), value.byteArrayOffset(), value.byteArrayLength());
        invalidate();
    }

    @Override
    public void putYear(long value) {
        checkState(AkType.YEAR);
        key.append(value);
        invalidate();
    }

    @Override
    public void putBool(boolean value) {
        checkState(AkType.BOOL);
        key.append(value);
        invalidate();
    }

    @Override
    public void putResultSet(Cursor value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AkType getConversionType() {
        return type;
    }

    // object interface

    @Override
    public String toString() {
        return key().toString();
    }

    // for use by this class

    protected final Key key() {
        return key;
    }

    // private methods

    private void checkState(AkType type) {
        ValueSourceHelper.checkType(this.type, type);
    }

    private void invalidate() {
        type = AkType.UNSUPPORTED;
    }

    // object state

    private Key key;
    private AkType type = AkType.UNSUPPORTED;
}
