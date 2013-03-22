
package com.akiban.util;

public interface ByteSource extends Comparable<ByteSource> {
    byte[] byteArray();
    int byteArrayOffset();
    int byteArrayLength();
    byte[] toByteSubarray();
}
