
package com.akiban.server.types3.pvalue;

import com.akiban.server.types3.TInstance;

public interface PBasicValueSource {
    TInstance tInstance();

    boolean isNull();

    boolean getBoolean();

    boolean getBoolean(boolean defaultValue);

    byte getInt8();

    short getInt16();

    char getUInt16();

    int getInt32();

    long getInt64();

    float getFloat();

    double getDouble();

    byte[] getBytes();

    String getString();
}
