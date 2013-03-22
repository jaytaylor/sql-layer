
package com.akiban.server.types;

import com.akiban.qp.operator.Cursor;
import com.akiban.util.ByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;

public interface ValueTarget {
    void putNull();
    void putDate(long value);
    void putDateTime(long value);
    void putDecimal(BigDecimal value);
    void putDouble(double value);
    void putFloat(float value);
    void putInt(long value);
    void putLong(long value);
    void putString(String value);
    void putText(String value);
    void putTime(long value);
    void putTimestamp(long value);
    void putInterval_Millis(long value);
    void putInterval_Month(long value);
    void putUBigInt(BigInteger value);
    void putUDouble(double value);
    void putUFloat(float value);
    void putUInt(long value);
    void putVarBinary(ByteSource value);
    void putYear(long value);
    void putBool(boolean value);
    void putResultSet(Cursor value);
    AkType getConversionType();
}
