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

package com.akiban.server.types;

import com.akiban.qp.operator.Cursor;
import com.akiban.server.Quote;
import com.akiban.server.types.extract.Extractors;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class FromObjectValueSource implements ValueSource {

    // FromObjectValueSource interface

    public FromObjectValueSource setExplicitly(Object object, AkType type) {
        setReflectively(object);
        
        if (akType != AkType.NULL && akType != type) {
            this.object = extractAs(type);
            akType = type;
        }
        return this;
    }

    public FromObjectValueSource setReflectively(Object object) {
        if (object == null) {
            setNull();
            return this;
        }

        if (object instanceof Character) {
             object = String.valueOf(object);
        }

        set(object, reflectivelyGetAkType(object));
        return this;
    }

    public void setNull() {
        this.akType = AkType.NULL;
        this.object = null;
    }

    // ValueSource interface

    @Override
    public boolean isNull() {
        return AkType.NULL.equals(akType);
    }

    @Override
    public BigDecimal getDecimal() {
        return as(BigDecimal.class, AkType.DECIMAL);
    }

    @Override
    public BigInteger getUBigInt() {
        return as(BigInteger.class, AkType.U_BIGINT);
    }

    @Override
    public ByteSource getVarBinary() {
        return as(ByteSource.class, AkType.VARBINARY);
    }

    @Override
    public double getDouble() {
        return as(Double.class, AkType.DOUBLE);
    }

    @Override
    public double getUDouble() {
        return as(Double.class, AkType.U_DOUBLE);
    }

    @Override
    public float getFloat() {
        return as(Float.class, AkType.FLOAT);
    }

    @Override
    public float getUFloat() {
        return as(Float.class, AkType.U_FLOAT);
    }

    @Override
    public long getDate() {
        return as(Long.class, AkType.DATE);
    }

    @Override
    public long getDateTime() {
        return as(Long.class, AkType.DATETIME);
    }

    @Override
    public long getInt() {
        return as(Long.class, AkType.INT);
    }

    @Override
    public long getLong() {
        return as(Long.class, AkType.LONG);
    }

    @Override
    public long getTime() {
        return as(Long.class, AkType.TIME);
    }

    @Override
    public long getTimestamp() {
        return as(Long.class, AkType.TIMESTAMP);
    }
    
    @Override
    public long getInterval_Millis() {
        return as(Long.class, AkType.INTERVAL_MILLIS);
    }

    @Override
    public long getInterval_Month() {
        return as(Long.class, AkType.INTERVAL_MONTH);
    }
    
    @Override
    public long getUInt() {
        return as(Long.class, AkType.U_INT);
    }

    @Override
    public long getYear() {
        return as(Long.class, AkType.YEAR);
    }

    @Override
    public String getString() {
        return as(String.class, AkType.VARCHAR);
    }

    @Override
    public String getText() {
        return as(String.class, AkType.TEXT);
    }

    @Override
    public boolean getBool() {
        return as(Boolean.class, AkType.BOOL);
    }

    @Override
    public Cursor getResultSet() {
        return as(Cursor.class, AkType.RESULT_SET);
    }

    @Override
    public void appendAsString(AkibanAppender appender, Quote quote) {
        AkType type = getConversionType();
        quote.quote(appender, type);
        if (type == AkType.UNSUPPORTED) {
            throw new IllegalStateException("source object not set");
        }
        quote.append(appender, String.valueOf(object));
        quote.quote(appender, type);
    }

    @Override
    public AkType getConversionType() {
        return akType;
    }

    // Object interface

    @Override
    public String toString() {
        return String.format("ValueSource(%s %s)", akType, object);
    }

    // class interface

    public static AkType reflectivelyGetAkType(Object object) {
        if (object == null)
            return AkType.NULL;
        if (object instanceof Integer || object instanceof Long)
            return AkType.LONG;
        else if (object instanceof String)
            return AkType.VARCHAR;
        else if (object instanceof Double)
            return AkType.DOUBLE;
        else if (object instanceof Float)
            return AkType.FLOAT;
        else if (object instanceof BigDecimal)
            return AkType.DECIMAL;
        else if (object instanceof ByteSource || object instanceof byte[])
            return AkType.VARBINARY;
        else if (object instanceof BigInteger)
             return AkType.U_BIGINT;
        else if (object instanceof Boolean)
             return AkType.BOOL;
        else if (object instanceof Character)
             return AkType.VARCHAR;
        else if (object instanceof Cursor)
             return AkType.RESULT_SET;
        else throw new UnsupportedOperationException("can't reflectively set " + object.getClass() + ": " + object);
    }

    // private methods

    private <T> T as(Class<T> castClass, AkType type) {
        ValueSourceHelper.checkType(akType, type);
        try {
            return castClass.cast(object);
        } catch (ClassCastException e) {
            String className = object == null ? "null" : object.getClass().getName();
            throw new ClassCastException("casting " + className + " to " + castClass);
        }
    }

    private Object extractAs(AkType type) {
        // TODO make a generic Extractor<T> so we can just use Extractors.get(type).getObject(this)
        switch (type) {
        case DATE:
        case DATETIME:
        case TIMESTAMP:
        case INTERVAL_MILLIS:
        case INTERVAL_MONTH:
        case INT:
        case LONG:
        case TIME:
        case U_INT:
        case YEAR:
            return Extractors.getLongExtractor(type).getLong(this);
        case U_DOUBLE:
        case DOUBLE:
            return Extractors.getDoubleExtractor().getDouble(this);
        case U_FLOAT:
        case FLOAT:
            return (float) Extractors.getDoubleExtractor().getDouble(this);
        case BOOL:
            return Extractors.getBooleanExtractor().getBoolean(this, null);
        case VARCHAR:
        case TEXT:
            return Extractors.getStringExtractor().getObject(this);
        case DECIMAL:
            return Extractors.getDecimalExtractor().getObject(this);
        case U_BIGINT:
            return Extractors.getUBigIntExtractor().getObject(this);
        case NULL:  return null;
        default:
            throw new UnsupportedOperationException("can't convert to type " + type);
//        case VARBINARY:
        }
    }

    private void set(Object object, AkType asType) {
        if (asType.equals(AkType.UNSUPPORTED)) {
            throw new IllegalArgumentException("can't set to UNSUPPORTED");
        }
        if (object == null) {
            setNull();
        }
        else {
            this.akType = asType;
            this.object = FromObjectTransformations.TRIVIAL_TRANSFORMATIONS.tryTransformations(asType, object);
        }
    }

    // object state

    private Object object;
    private AkType akType = AkType.UNSUPPORTED;
}
