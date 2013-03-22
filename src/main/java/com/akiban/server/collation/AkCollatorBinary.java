package com.akiban.server.collation;

import com.akiban.server.types.ValueSource;
import com.persistit.Key;

public class AkCollatorBinary extends AkCollator {

    public AkCollatorBinary() {
        super(AkCollatorFactory.UCS_BINARY, AkCollatorFactory.UCS_BINARY, 0);
    }
    
    @Override
    public boolean isRecoverable() {
        return true;
    }

    @Override
    public void append(Key key, String value) {
        key.append(value);
    }

    @Override
    public String decode(Key key) {
        return key.decodeString();
    }

    /**
     * Append the given value to the given key.
     */
    public byte[] encodeSortKeyBytes(String value) {
        throw new UnsupportedOperationException("No sort key encoding for binary collation");
    }

    /**
     * Recover the value or throw an unsupported exception.
     */
    public String decodeSortKeyBytes(byte[] bytes, int index, int length) {
        throw new UnsupportedOperationException("No sort key encoding for binary collation");
    }

    @Override
    public int compare(String string1, String string2) {
        return string1.compareTo(string2);
    }

    @Override
    public boolean isCaseSensitive() {
        return true;
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public int hashCode(String string) {
        return string.hashCode();
    }
}
