package com.akiban.server;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.persistit.Value;

public final class PersistitValuePValueTarget implements PValueTarget {
    
    // PersistitValuePValueTarget interface
    
    public void attach(Value value) {
        this.value = value;
    }
    
    // PValueTarget interface
    
    @Override
    public boolean supportsCachedObjects() {
        return false;
    }

    @Override
    public void putObject(Object object) {
        value.put(object);
    }

    @Override
    public TInstance tInstance() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putNull() {
        value.putNull();
    }

    @Override
    public void putBool(boolean value) {
        this.value.put(value);
    }

    @Override
    public void putInt8(byte value) {
        this.value.put(value);
    }

    @Override
    public void putInt16(short value) {
        this.value.put(value);
    }

    @Override
    public void putUInt16(char value) {
        this.value.put(value);
    }

    @Override
    public void putInt32(int value) {
        this.value.put(value);
    }

    @Override
    public void putInt64(long value) {
        this.value.put(value);
    }

    @Override
    public void putFloat(float value) {
        this.value.put(value);
    }

    @Override
    public void putDouble(double value) {
        this.value.put(value);
    }

    @Override
    public void putBytes(byte[] value) {
        this.value.put(value);
    }

    @Override
    public void putString(String value, AkCollator collator) {
        this.value.put(value);
    }
    
    private Value value;
}
