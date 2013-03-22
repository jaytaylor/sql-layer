
package com.akiban.server.types3.pvalue;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.types3.TInstance;

public interface PBasicValueTarget {
    TInstance tInstance();

    void putNull();

    void putBool(boolean value);

    void putInt8(byte value);

    void putInt16(short value);

    void putUInt16(char value);

    void putInt32(int value);

    void putInt64(long value);

    void putFloat(float value);

    void putDouble(double value);

    void putBytes(byte[] value);

    void putString(String value, AkCollator collator);
}
