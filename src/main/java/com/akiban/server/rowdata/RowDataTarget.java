
package com.akiban.server.rowdata;

public interface RowDataTarget {
    void bind(FieldDef fieldDef, byte[] backingBytes, int offset);
    int lastEncodedLength();
    void putNull();
}
